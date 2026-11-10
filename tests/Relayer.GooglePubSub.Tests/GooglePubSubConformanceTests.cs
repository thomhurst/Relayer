using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Grpc.Core;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.GooglePubSub.Tests;

[NotInParallel("PubSubEmulator")]
public class GooglePubSubConformanceTests
{
    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.Retry)]
    [Arguments(TransportConformanceCase.DeadLetter)]
    [Arguments(TransportConformanceCase.Delay)]
    [Arguments(TransportConformanceCase.RedeliveryDelay)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.Reconnect)]
    [Arguments(TransportConformanceCase.SendDuringOutage)]
    public async Task PubSub_passes_or_explicitly_skips_unsupported_capability(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<GooglePubSubDelivery, GooglePubSubBinding>(PubSubFixture.CreateAsync, TimeSpan.FromMinutes(2));
        var result = await suite.RunAsync(scenario);
        var unsupported = scenario is TransportConformanceCase.Retry or TransportConformanceCase.DeadLetter or TransportConformanceCase.Delay or TransportConformanceCase.RedeliveryDelay;
        await Assert.That(result.Skipped).IsEqualTo(unsupported);
        if (unsupported) await Assert.That(result.SkipReason).IsNotNull();
    }
}

internal sealed class PubSubFixture : ITransportConformanceFixture<GooglePubSubDelivery>
{
    internal PubSubBroker Broker { get; }
    private readonly ServiceProvider _provider;
    internal GooglePubSubTransport Adapter { get; }
    private GooglePubSubConsumer? _worker;
    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(10);
    public bool IsExpectedOutageFailure(Exception error) => error is RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded };
    public string Destination { get; } = TopicName.FromProjectTopic("relayer-test", "conformance-" + Guid.NewGuid().ToString("N")).ToString();
    internal string Subscription => Destination.Replace("/topics/", "/subscriptions/", StringComparison.Ordinal);
    public ITransport Transport => Adapter;
    public string MessagingSystem => "gcp_pubsub";
    public string SettlementOperation => "ack";
    public HeaderNames HeaderNames { get; } = new("ce-");

    private PubSubFixture(PubSubBroker broker, MessageTracing tracing)
    {
        Broker = broker;
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.GooglePubSub("conformance", transport =>
        {
            transport.TopologyMode = TopologyMode.Provision;
            transport.Options.Configure(options =>
            {
                // PublishAsync has no caller cancellation parameter. Bound retries in the native API client.
                var publisherSettings = new PublisherServiceApiSettings();
                publisherSettings.PublishSettings = publisherSettings.PublishSettings.WithExpiration(Expiration.FromTimeout(TimeSpan.FromSeconds(5)));
                options.Publisher = new PublisherServiceApiClientImpl(broker.Publisher.GrpcClient, publisherSettings, null);
                options.Subscriber = broker.Subscriber;
                options.PublisherSettings = new() { EnableMessageOrdering = true };
                options.Topics.Add(TopicName.Parse(Destination));
                options.Subscriptions.Add(new() { Name = Subscription, Topic = Destination, EnableMessageOrdering = true, AckDeadlineSeconds = 10 });
            });
        }));
        // Use the source-local tracing owner supplied by the conformance suite.
        services.AddSingleton(tracing);
        _provider = services.BuildServiceProvider();
        Adapter = (GooglePubSubTransport)_provider.GetRequiredService<ITransport>();
    }

    internal static async ValueTask<ITransportConformanceFixture<GooglePubSubDelivery>> CreateAsync(MessageTracing tracing, CancellationToken token)
    {
        var broker = await PubSubBroker.StartAsync(token);
        PubSubFixture? fixture = null;
        try
        {
            fixture = new(broker, tracing);
            var report = await fixture.Adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
            if (report.Differences.Count != 0) throw new InvalidOperationException(string.Join("; ", report.Differences));
            return fixture;
        }
        catch
        {
            if (fixture is null) await broker.DisposeAsync();
            else await fixture.DisposeAsync();
            throw;
        }
    }

    public void RegisterConsumer(Func<GooglePubSubDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        _worker = Adapter.AddConsumer(Subscription, new() { FlowControlSettings = new(32, null) });
        _worker.Receive = receive;
    }
    public ValueTask SettleAsync(GooglePubSubDelivery incoming, Disposition disposition, CancellationToken token) => _worker!.SettleAsync(incoming, disposition, token);
    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token) => throw new NotSupportedException("Pub/Sub cannot dead-letter on demand with error attributes.");
    // Closing the proxy drops live TCP streams while preserving the emulator's native endpoint state.
    public Task DisconnectAsync(CancellationToken token) => Broker.SetConnectedAsync(false, token);
    public Task ReconnectAsync(CancellationToken token) => Broker.SetConnectedAsync(true, token);
    public async ValueTask DisposeAsync()
    {
        try { await _provider.DisposeAsync(); }
        finally { await Broker.DisposeAsync(); }
    }
}
