#pragma warning disable RLY001 // Cached serialization isolates composition costs from JSON.
using System.Buffers;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Production failure composition versus the explicit delegate and identical scoped application work.</summary>
/// <remarks>
/// Both pipelines attach tracing and telemetry with no listeners (D25). Suspension is deterministic:
/// the handler returns a pending source, and the driver completes it after dispatch has registered its continuation.
/// No Task, thread-pool scheduling or asynchronous benchmark wrapper obscures continuation allocations.
/// DI scope, handler and service-cache allocations are application lifetime costs (D52).
/// </remarks>
[MemoryDiagnoser]
[BenchmarkCategory("RegressionGate")]
public class FailureConsumePipelineBenchmarks
{
    private ServiceProvider _provider = null!;
    private ConsumePipeline _explicit = null!;
    private ConsumePipeline _failures = null!;
    private HandlerDescriptor<Payload> _descriptor = null!;
    private readonly Serializer _serializer = new();
    private readonly Completion _completion = new();
    private readonly DirectContext _direct = new();
    private readonly ConsumeMessage _message = new()
    {
        MessageId = "benchmark", MessageType = new("benchmark.failure-consume", 1), Body = ReadOnlySequence<byte>.Empty,
    };

    [Params(false, true)]
    public bool SuspendHandler { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _completion.Suspend = SuspendHandler;
        var services = new ServiceCollection();
        services.AddSingleton(_completion);
        services.AddScoped<Handler>();
        services.AddRelayer(r =>
        {
            r.MessageTypes.Add<Payload>("benchmark.failure-consume");
            r.Handler<Handler>();
            r.InMemory("benchmark", transport => transport.Consume("source", consumer => consumer.Handler<Handler>()));
        });
        _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        _descriptor = _provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>();
        var scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        var types = _provider.GetRequiredService<MessageTypeRegistry>();
        var tracing = _provider.GetRequiredService<MessageTracing>();
        var endpoint = new MessagingEndpoint { System = "relayer.inmemory", Destination = "source", ConsumerGroup = "consumer" };
        var telemetry = _provider.GetRequiredService<RelayerInstrumentation>().CreateEndpoint(new()
        {
            System = endpoint.System, Destination = endpoint.Destination, ConsumerGroup = endpoint.ConsumerGroup,
        });
        var failures = new FailurePipeline(_provider, "benchmark", "source", "consumer");
        _explicit = new ConsumePipelineBuilder(scopes, types, _serializer,
                static (_, error, _) => ValueTask.FromException(error))
            .Handler(_descriptor).Tracing(tracing, endpoint).Telemetry(telemetry).Build(1);
        _failures = new ConsumePipelineBuilder(scopes, types, _serializer, failures)
            .Handler(_descriptor).Tracing(tracing, endpoint).Telemetry(telemetry).Build(1);
    }

    [Benchmark(Baseline = true)]
    public void DirectScopedDispatch()
    {
        using var scope = _provider.CreateScope();
        _direct.Provider = scope.ServiceProvider;
        var handler = _descriptor.Resolve(scope.ServiceProvider);
        var pending = _descriptor.DispatchAsync(handler, _serializer.Deserialize<Payload>(_message.Body), _direct, default);
        Complete(pending);
    }

    [Benchmark]
    public void ExplicitFailureDelegate() => Consume(_explicit);

    [Benchmark]
    public void ProductionFailurePipeline() => Consume(_failures);

    private void Consume(ConsumePipeline pipeline)
    {
        using var context = pipeline.RentContext(_message);
        Complete(pipeline.ProcessAsync(context));
    }

    private void Complete(ValueTask pending)
    {
        if (SuspendHandler)
        {
            if (pending.IsCompleted) throw new InvalidOperationException("Handler must suspend before completion.");
            _completion.Complete();
        }
        if (!pending.IsCompleted) throw new InvalidOperationException("Dispatch must finish when its handler completes.");
        pending.GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public Task Cleanup() => _provider.DisposeAsync().AsTask();

    public sealed class Payload;

    public sealed class Handler(Completion completion) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => completion.Wait();
    }

    public sealed class Completion : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _source;
        internal bool Suspend { get; set; }
        internal ValueTask Wait()
        {
            if (!Suspend) return default;
            _source.Reset();
            return new(this, _source.Version);
        }
        internal void Complete() => _source.SetResult(true);
        public void GetResult(short token) => _source.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _source.OnCompleted(continuation, state, token, flags);
    }

    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }

    private sealed class DirectContext : MessageContext
    {
        internal IServiceProvider Provider { get; set; } = null!;
        public override string MessageId => "benchmark";
        public override MessageType MessageType => new("benchmark.failure-consume", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => Provider;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
