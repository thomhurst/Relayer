using System.Buffers;
using Relayer.Pipeline;
using Relayer.Transports;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

[MemoryDiagnoser]
public class FailureSettlementBenchmarks
{
    [Params(1, 10)] public int Pending { get; set; }
    private ServiceProvider _provider = null!;
    private ConsumePipeline _pipeline = null!;
    private IServiceScopeFactory _scopes = null!;
    private HandlerDescriptor<Payload> _descriptor = null!;
    private readonly Serializer _serializer = new();
    private DefaultMessageContext[] _contexts = null!;
    private ValueTask[] _processing = null!;
    private Gate[] _gates = null!;
    private int _next;
    private readonly ConsumeMessage _message = new() { MessageId = "id", MessageType = new("benchmark.settlement", 1), Body = default };

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport, Transport>();
        services.AddRelayer(r =>
        {
            r.Infrastructure().MessageTypes.Add<Payload>("benchmark.settlement");
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>();
        });
        _provider = services.BuildServiceProvider();
        _scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        _descriptor = _provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>();
        var failures = new FailurePipeline(_provider, "benchmark", "source", "consumer");
        _pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _serializer, failures)
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>())
            .Settle(_ => _gates[_next++].Wait()).Build(Pending);
        _contexts = new DefaultMessageContext[Pending];
        _processing = new ValueTask[Pending];
        _gates = new Gate[Pending];
        for (var i = 0; i < Pending; i++) _gates[i] = new();
    }

    [Benchmark(Baseline = true)]
    public void DirectScopedWindow()
    {
        for (var i = 0; i < Pending; i++)
        {
            var scope = new AsyncServiceScope(_scopes.CreateScope());
            var handler = _descriptor.Resolve(scope.ServiceProvider);
            _descriptor.DispatchAsync(handler, _serializer.Deserialize<Payload>(default), null!, default).GetAwaiter().GetResult();
            scope.DisposeAsync().GetAwaiter().GetResult(); // This fixture has no asynchronous disposers.
        }
    }

    [Benchmark]
    public void SettlementWindow()
    {
        _next = 0;
        for (var i = 0; i < Pending; i++)
        {
            _gates[i].Reset();
            _contexts[i] = _pipeline.RentContext(_message);
            _processing[i] = _pipeline.ProcessAsync(_contexts[i]);
        }
        for (var i = 0; i < Pending; i++) _gates[i].Complete();
        for (var i = 0; i < Pending; i++)
        {
            _processing[i].GetAwaiter().GetResult(); // All gates completed synchronously above.
            _contexts[i].Dispose();
        }
    }

    [GlobalCleanup] public async Task Cleanup() => await _provider.DisposeAsync();
    [Message("benchmark.settlement")] public sealed class Payload;
    public sealed class Handler : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) { }
    }
    private sealed class Transport : ITransport
    {
        public string Name => "benchmark";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { DeadLetter = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => default;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => default;
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
    private sealed class Gate : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _completion;
        internal void Reset() => _completion.Reset();
        internal ValueTask Wait() => new(this, _completion.Version);
        internal void Complete() => _completion.SetResult(true);
        public void GetResult(short token) => _completion.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => _completion.OnCompleted(continuation, state, token, flags);
    }
}
