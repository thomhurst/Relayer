using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Grpc.Core;
using System.Reflection;
using Google.Cloud.PubSub.V1;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
    [Arguments(TransportConformanceCase.CleanShutdown)]
    public async Task PubSub_passes_or_explicitly_skips_unsupported_capability(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<GooglePubSubDelivery, GooglePubSubBinding>(PubSubFixture.CreateAsync, TimeSpan.FromMinutes(2));
        var result = await suite.RunAsync(scenario);
        var unsupported = scenario is TransportConformanceCase.Retry or TransportConformanceCase.DeadLetter or TransportConformanceCase.Delay or TransportConformanceCase.RedeliveryDelay;
        await Assert.That(result.Skipped).IsEqualTo(unsupported);
        if (unsupported) await Assert.That(result.SkipReason).IsNotNull();
    }
}

internal sealed class PubSubFixture : ITransportConformanceFixture<GooglePubSubDelivery>, ITransportShutdownConformanceFixture
{
    internal PubSubBroker Broker { get; }
    private readonly ServiceProvider _provider;
    internal GooglePubSubTransport Adapter { get; }
    private GooglePubSubConsumer? _worker;
    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(10);
    public bool IsExpectedOutageFailure(Exception error) => error is RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded };
    private readonly ILoggerFactory _logs;
    private readonly PubSubShutdownInterceptor _native = new();
    private readonly SubscriberServiceApiClient _subscriber;
    private readonly MessageTracing _tracing;
    private bool _verifyShutdown;
    public ConformanceLogCapture Logs { get; }
    public string Destination { get; } = TopicName.FromProjectTopic("relayer-test", "conformance-" + Guid.NewGuid().ToString("N")).ToString();
    internal string Subscription => Destination.Replace("/topics/", "/subscriptions/", StringComparison.Ordinal);
    public ITransport Transport => Adapter;
    public string MessagingSystem => "gcp_pubsub";
    public string SettlementOperation => "ack";
    public HeaderNames HeaderNames { get; } = new("ce-");

    private PubSubFixture(PubSubBroker broker, MessageTracing tracing, ConformanceLogCapture logs, ILoggerFactory loggerFactory)
    {
        Broker = broker;
        _tracing = tracing;
        Logs = logs;
        _logs = loggerFactory;
        var invoker = broker.Invoker.Intercept(_native);
        var publisherSettings = new PublisherServiceApiSettings();
        publisherSettings.PublishSettings = publisherSettings.PublishSettings.WithExpiration(Expiration.FromTimeout(TimeSpan.FromSeconds(5)));
        var publisher = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(invoker), publisherSettings, _logs.CreateLogger("PubSub.PublisherApi"));
        _subscriber = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(invoker), null, _logs.CreateLogger("PubSub.SubscriberApi"));
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services.AddRelayer(builder => builder.GooglePubSub("conformance", transport =>
        {
            transport.TopologyMode = TopologyMode.Provision;
            transport.Options.Configure(options =>
            {
                options.Publisher = publisher;
                options.Subscriber = _subscriber;
                options.PublisherSettings = WithNativeLogger(new PublisherClient.Settings { EnableMessageOrdering = true }, _logs.CreateLogger("PubSub.Publisher"));
                options.Topics.Add(TopicName.Parse(Destination));
                options.Subscriptions.Add(new() { Name = Subscription, Topic = Destination, EnableMessageOrdering = true, AckDeadlineSeconds = 10 });
            });
        }));
        // Use the source-local tracing owner supplied by the conformance suite.
        services.AddSingleton(tracing);
        _provider = services.BuildServiceProvider();
        Adapter = (GooglePubSubTransport)_provider.GetRequiredService<ITransport>();
    }

    // These prepared-client constructors are production's native seam. Google only exposes
    // Settings.Logger through builders, which would replace those clients/channels. Configure
    // the pinned SDK's internal logger here, in test setup only; clones preserve it.
    private static T WithNativeLogger<T>(T settings, ILogger logger)
    {
        typeof(T).GetProperty("Logger", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(settings, logger);
        return settings;
    }

    internal static async ValueTask<ITransportConformanceFixture<GooglePubSubDelivery>> CreateAsync(MessageTracing tracing, CancellationToken token)
    {
        var logs = new ConformanceLogCapture();
        var loggerFactory = LoggerFactory.Create(builder => builder.AddProvider(logs));
        PubSubBroker broker;
        try { broker = await PubSubBroker.StartAsync(token, loggerFactory); }
        catch { loggerFactory.Dispose(); throw; }
        PubSubFixture? fixture = null;
        try
        {
            fixture = new(broker, tracing, logs, loggerFactory);
            var report = await fixture.Adapter.ApplyTopologyAsync(TopologyMode.Provision, token);
            if (report.Differences.Count != 0) throw new InvalidOperationException(string.Join("; ", report.Differences));
            return fixture;
        }
        catch
        {
            if (fixture is null) { await broker.DisposeAsync(); loggerFactory.Dispose(); }
            else await fixture.DisposeAsync();
            throw;
        }
    }

    public void RegisterConsumer(Func<GooglePubSubDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        var settings = WithNativeLogger(new SubscriberClient.Settings { FlowControlSettings = new(32, null) }, _logs.CreateLogger("PubSub.Subscriber"));
        _worker = new(() => new ObservedSubscriberClient(
            new SubscriberClientImpl(SubscriptionName.Parse(Subscription), [_subscriber], settings.Clone(), static () => Task.CompletedTask), _native),
            _tracing, Destination, Adapter, Subscription);
        Adapter.Consumers.Add(_worker);
        _worker.Receive = receive;
    }
    public ValueTask SettleAsync(GooglePubSubDelivery incoming, Disposition disposition, CancellationToken token) => _worker!.SettleAsync(incoming, disposition, token);
    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token) => throw new NotSupportedException("Pub/Sub cannot dead-letter on demand with error attributes.");
    // Closing the proxy drops live TCP streams while preserving the emulator's native endpoint state.
    public Task DisconnectAsync(CancellationToken token) => Broker.SetConnectedAsync(false, token);
    public Task ReconnectAsync(CancellationToken token) => Broker.SetConnectedAsync(true, token);
    public async ValueTask VerifyShutdownAsync(CancellationToken token)
    {
        _verifyShutdown = true;
        if (!_worker!.Completion.IsCompletedSuccessfully)
            throw new InvalidOperationException("Native Pub/Sub subscriber did not drain.");
        _native.AssertClean();
        // In addition to the successful native Ack RPC, verify the broker still owns the
        // subscription used for native redelivery if acknowledgement outcome is ambiguous.
        var persisted = await _subscriber.GetSubscriptionAsync(Subscription, cancellationToken: token);
        if (persisted.Topic != Destination) throw new InvalidOperationException("The durable Pub/Sub subscription disappeared.");
    }

    public async ValueTask DisposeAsync()
    {
        try { await _provider.DisposeAsync(); }
        finally
        {
            _native.PublisherClosed();
            try { await Broker.DisposeAsync(); }
            finally
            {
                _logs.Dispose();
                if (_verifyShutdown) _native.AssertClean(requireClosure: true);
            }
        }
    }
}
