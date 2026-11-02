using System.Buffers;
using BenchmarkDotNet.Attributes;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.GooglePubSub;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Native streaming pull, default JSON and drained acknowledgements versus the production receive adapter.</summary>
/// <remarks>Both paths use the same native settings, JSON contract, handler and fresh 512-message subscription workload.</remarks>
[MemoryDiagnoser]
public class GooglePubSubBrokerReceiveBenchmarks
{
    private const int Records = 512;
    private readonly Task<SubscriberClient.Reply> _ack = Task.FromResult(SubscriberClient.Reply.Ack);
    private readonly PubSubReceiveCounter _counter = new();
    private readonly MessageTracing _tracing = new(Options.Create(new TracingOptions()), []);
    private readonly Task<string>[] _pending = new Task<string>[Records];
    private GrpcChannel _channel = null!;
    private PublisherServiceApiClient _publisherApi = null!;
    private SubscriberServiceApiClient _subscriberApi = null!;
    private PublisherClient _publisher = null!;
    private ServiceProvider _provider = null!;
    private IServiceScopeFactory _scopes = null!;
    private IMessageSerializer _serializer = null!;
    private GooglePubSubConsumer _worker = null!;
    private SubscriberClient? _raw;
    private SubscriberClient? _prepared;
    private string _topic = null!;
    private string _subscription = null!;
    private readonly SubscriberClient.Settings _settings = new() { FlowControlSettings = new(32, null) };

    [GlobalSetup]
    public async Task Setup()
    {
        var address = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST") ?? throw new InvalidOperationException("Set PUBSUB_EMULATOR_HOST to a Pub/Sub emulator.");
        _channel = GrpcChannel.ForAddress("http://" + address);
        _publisherApi = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(_channel), null, null);
        _subscriberApi = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(_channel), null, null);
        _topic = TopicName.FromProjectTopic("relayer-test", "receive-benchmark-" + Guid.NewGuid().ToString("N")).ToString();
        _subscription = _topic.Replace("/topics/", "/subscriptions/", StringComparison.Ordinal);
        await _publisherApi.CreateTopicAsync(_topic);
        _publisher = new PublisherClientImpl(TopicName.Parse(_topic), [_publisherApi], new(), static () => Task.CompletedTask);
        var services = new ServiceCollection();
        services.AddSingleton(_counter);
        services.AddSingleton<PubSubBrokerBenchmarkHandler>();
        services.AddRelayer(relayer =>
        {
            relayer.MessageTypes.Add<PubSubBenchmarkOrder>("benchmarks.pubsub.order");
            relayer.UseJsonSerializer(PubSubBenchmarkJson.Default);
            relayer.Handler<PubSubBrokerBenchmarkHandler>();
        });
        _provider = services.BuildServiceProvider();
        _scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
        var pipeline = new ConsumePipelineBuilder(_scopes, _provider.GetRequiredService<MessageTypeRegistry>(),
            _serializer, static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<PubSubBenchmarkOrder, PubSubBrokerBenchmarkHandler>()).Build(32);
        var receive = new ReceivePipeline<GooglePubSubDelivery, GooglePubSubBinding>(pipeline, new("ce-"), 32);
        _worker = new(() => _prepared!, _tracing, _topic, null!) { Receive = receive.ProcessAsync };
    }

    [IterationSetup]
    public void PrepareWorkload() => PrepareWorkloadAsync().GetAwaiter().GetResult();

    // BenchmarkDotNet requires synchronous iteration hooks. All broker preparation stays outside measurement.
    private async Task PrepareWorkloadAsync()
    {
        if (_raw is not null) await _raw.DisposeAsync();
        if (_prepared is not null) await _prepared.DisposeAsync();
        try { await _subscriberApi.DeleteSubscriptionAsync(_subscription); }
        catch (Grpc.Core.RpcException error) when (error.StatusCode == Grpc.Core.StatusCode.NotFound) { }
        await _subscriberApi.CreateSubscriptionAsync(new Subscription { Name = _subscription, Topic = _topic, AckDeadlineSeconds = 60 });
        var message = new PubsubMessage { Data = ByteString.CopyFromUtf8("{\"Value\":42}") };
        message.Attributes["ce-type"] = "benchmarks.pubsub.order;v=1";
        message.Attributes["ce-id"] = "benchmark";
        message.Attributes["ce-source"] = "/";
        message.Attributes["ce-specversion"] = "1.0";
        for (var i = 0; i < Records; i++) _pending[i] = _publisher.PublishAsync(message);
        await Task.WhenAll(_pending);
        _raw = NewSubscriber();
        _prepared = NewSubscriber();
        _counter.Begin(Records);
    }

    private SubscriberClient NewSubscriber() => new SubscriberClientImpl(SubscriptionName.Parse(_subscription), [_subscriberApi], _settings, static () => Task.CompletedTask);

    [Benchmark(Baseline = true, OperationsPerInvoke = Records)]
    public async Task NativeStreamingPull()
    {
        var completion = _raw!.StartAsync(RawCallback);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await _counter.Completion.WaitAsync(timeout.Token);
        await _raw.StopAsync(new SubscriberClient.ShutdownOptions(), timeout.Token);
        await completion;
        await _raw.DisposeAsync();
        _raw = null;
    }

    private Task<SubscriberClient.Reply> RawCallback(PubsubMessage message, CancellationToken token)
    {
        using var scope = _scopes.CreateScope();
        var payload = _serializer.Deserialize<PubSubBenchmarkOrder>(new ReadOnlySequence<byte>(message.Data.Memory));
        scope.ServiceProvider.GetRequiredService<PubSubBrokerBenchmarkHandler>().Record(payload);
        return _ack;
    }

    [Benchmark(OperationsPerInvoke = Records)]
    public async Task RelayerStreamingPull()
    {
        _worker.Start();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await _counter.Completion.WaitAsync(timeout.Token);
        await _worker.StopAsync(timeout.Token);
        _prepared = null;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_worker is not null) await _worker.DisposeAsync();
        if (_raw is not null) await _raw.DisposeAsync();
        if (_prepared is not null) await _prepared.DisposeAsync();
        if (_publisher is not null) await _publisher.DisposeAsync();
        if (_provider is not null) await _provider.DisposeAsync();
        if (_subscription is not null) await _subscriberApi.DeleteSubscriptionAsync(_subscription);
        if (_topic is not null) await _publisherApi.DeleteTopicAsync(_topic);
        _channel?.Dispose();
        _tracing.Dispose();
    }
}

public sealed class PubSubReceiveCounter
{
    private int _count;
    private int _target;
    private TaskCompletionSource _completion = null!;
    internal Task Completion => _completion.Task;
    internal void Begin(int count) { _count = 0; _target = count; _completion = new(TaskCreationOptions.RunContinuationsAsynchronously); }
    internal void Record(PubSubBenchmarkOrder message)
    {
        if (message.Value != 42) throw new InvalidOperationException("Unexpected benchmark payload.");
        if (Interlocked.Increment(ref _count) == _target) _completion.TrySetResult();
    }
}
public sealed class PubSubBrokerBenchmarkHandler(PubSubReceiveCounter counter) : IHandler<PubSubBenchmarkOrder>
{
    internal void Record(PubSubBenchmarkOrder message) => counter.Record(message);
    public ValueTask HandleAsync(PubSubBenchmarkOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        Record(message);
        return default;
    }
}
