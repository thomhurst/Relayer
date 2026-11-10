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
    private ServiceProvider _provider = null!;
    internal GooglePubSubTransport Adapter { get; private set; } = null!;
    private GooglePubSubConsumer? _worker;
    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(10);
    private bool _observedNativeFailure;
    private bool _disconnected;
    public bool IsExpectedOutageFailure(Exception error)
    {
        if (IsExpectedPublishFailure(error)) return true;
        if (_worker?.Completion.Exception is not { } failure || !IsNativeStreamWriteFailure(failure)
            || !IsObservedSubscriberFailure(error, failure)) return false;
        _observedNativeFailure = true;
        return true;
    }
    private readonly ILoggerFactory _logs;
    private PubSubShutdownInterceptor _native = new();
    private SubscriberServiceApiClient _subscriber = null!;
    private Func<GooglePubSubDelivery, CancellationToken, ValueTask<Disposition>>? _receive;
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
        BuildApplication();
    }

    private void BuildApplication()
    {
        var invoker = Broker.Invoker.Intercept(_native);
        var publisherSettings = new PublisherServiceApiSettings();
        publisherSettings.PublishSettings = publisherSettings.PublishSettings.WithExpiration(Expiration.FromTimeout(TimeSpan.FromSeconds(5)));
        var publisher = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(invoker), publisherSettings, _logs.CreateLogger("PubSub.PublisherApi"));
        _subscriber = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(invoker), null, _logs.CreateLogger("PubSub.SubscriberApi"));
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(Logs));
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
        services.AddSingleton(_tracing);
        _provider = services.BuildServiceProvider();
        Adapter = (GooglePubSubTransport)_provider.GetRequiredService<ITransport>();
        NativePublisherShutdown.Observe(Adapter.PublisherFor(Destination), _native);
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
        _receive = receive;
        var settings = WithNativeLogger(new SubscriberClient.Settings { FlowControlSettings = new(32, null) }, _logs.CreateLogger("PubSub.Subscriber"));
        _worker = new(() => new ObservedSubscriberClient(
            new SubscriberClientImpl(SubscriptionName.Parse(Subscription), [_subscriber], settings.Clone(), static () => Task.CompletedTask), _native),
            _tracing, Destination, Adapter, Subscription);
        Adapter.Consumers.Add(_worker);
        _worker.Receive = receive;
    }
    internal Task ConsumerCompletion => _worker!.Completion;
    internal void InjectInitialWriteFailure(IOException error) => _native.InitialWriteFailure = error;

    public ValueTask SettleAsync(GooglePubSubDelivery incoming, Disposition disposition, CancellationToken token) => _worker!.SettleAsync(incoming, disposition, token);
    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token) => throw new NotSupportedException("Pub/Sub cannot dead-letter on demand with error attributes.");
    // Closing the proxy drops live TCP streams while preserving the emulator's native endpoint state.
    public async Task DisconnectAsync(CancellationToken token)
    {
        // StartAsync returns before the native initial write. Cut an established subscriber,
        // rather than racing that write into the client's documented fatal startup path.
        await _native.WaitForInitialWriteAsync(token);
        await Broker.SetConnectedAsync(false, token);
        _disconnected = true;
    }
    public async Task ReconnectAsync(CancellationToken token)
    {
        await Broker.SetConnectedAsync(true, token);
        var wasDisconnected = _disconnected;
        _disconnected = false;
        var failure = _worker?.Completion.Exception;
        if (failure is null) return;
        // Google's client intentionally makes an initial StreamingPull write I/O failure fatal
        // (googleapis/google-cloud-dotnet#16088). Recreate the application explicitly, retaining
        // the real broker, topic and subscription; never add a subscriber retry loop to Relayer.
        if ((!_observedNativeFailure && !wasDisconnected) || !IsNativeStreamWriteFailure(failure)) throw failure;
        // Reconnect runs with a live application; the outage-send case already observed
        // its failure during StopAsync and starts the replacement explicitly.
        var restart = !_observedNativeFailure;
        try { await _provider.DisposeAsync(); }
        catch (Exception error) when (IsObservedSubscriberFailure(error, failure)) { }
        _observedNativeFailure = false;
        _native = new();
        BuildApplication();
        RegisterConsumer(_receive!);
        var persisted = await _subscriber.GetSubscriptionAsync(Subscription, cancellationToken: token);
        if (persisted.Topic != Destination) throw new InvalidOperationException("Recovery replaced the durable Pub/Sub endpoint.");
        if (restart) await Adapter.StartAsync(token);
    }

    private static bool IsNativeStreamWriteFailure(Exception error) => error is AggregateException aggregate
        ? aggregate.InnerExceptions.All(IsNativeStreamWriteFailure)
        : error is IOException && ContainsDisconnectedConnection(error);

    internal static bool IsExpectedPublishFailure(Exception error) => error is RpcException rpc
        && (rpc.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded
            || (rpc.StatusCode == StatusCode.Internal && rpc.Status.DebugException is { } cause
                && ContainsDisconnectedConnection(cause)));

    private static bool ContainsDisconnectedConnection(Exception error) =>
        error is System.Net.Sockets.SocketException { SocketErrorCode: System.Net.Sockets.SocketError.ConnectionReset or System.Net.Sockets.SocketError.ConnectionAborted }
        || error is System.Net.Http.HttpIOException { HttpRequestError: System.Net.Http.HttpRequestError.InvalidResponse or System.Net.Http.HttpRequestError.ResponseEnded }
        || (error.InnerException is { } inner && ContainsDisconnectedConnection(inner));

    private static bool IsObservedSubscriberFailure(Exception error, AggregateException failure) => error is AggregateException aggregate
        ? aggregate.InnerExceptions.All(inner => IsObservedSubscriberFailure(inner, failure))
        : failure.Flatten().InnerExceptions.Any(observed => ReferenceEquals(observed, error));
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
            try { await Broker.DisposeAsync(); }
            finally
            {
                _logs.Dispose();
                if (_verifyShutdown) _native.AssertClean(requireClosure: true);
            }
        }
    }
}
