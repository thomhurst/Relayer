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
    [Params(false, true)] public bool PendingHandler { get; set; }
    private ServiceProvider _provider = null!;
    private ConsumePipeline _pipeline = null!;
    private IServiceScopeFactory _scopes = null!;
    private HandlerDescriptor<Payload> _descriptor = null!;
    private readonly Serializer _serializer = new();
    private DefaultMessageContext[] _contexts = null!;
    private ValueTask[] _processing = null!;
    private Gate[] _gates = null!;
    private int _next;
    private AsyncServiceScope[] _directScopes = null!;
    private readonly HandlerGates _handlerGates = new();
    private readonly ConsumeMessage _message = new() { MessageId = "id", MessageType = new("benchmark.settlement", 1), Body = default };

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_handlerGates);
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
            .Settle(_ => PendingHandler ? default : _gates[_next++].Wait()).Build(Pending);
        _contexts = new DefaultMessageContext[Pending];
        _processing = new ValueTask[Pending];
        _gates = new Gate[Pending];
        for (var i = 0; i < Pending; i++) _gates[i] = new();
        _directScopes = new AsyncServiceScope[Pending];
        _handlerGates.Gates = PendingHandler ? _gates : null;
    }

    [Benchmark(Baseline = true)]
    public void DirectScopedWindow()
    {
        ResetGates();
        for (var i = 0; i < Pending; i++)
        {
            _directScopes[i] = new AsyncServiceScope(_scopes.CreateScope());
            var handler = _descriptor.Resolve(_directScopes[i].ServiceProvider);
            _processing[i] = _descriptor.DispatchAsync(handler, _serializer.Deserialize<Payload>(default), null!, default);
            if (PendingHandler && _processing[i].IsCompleted)
                throw new InvalidOperationException("The direct handler must genuinely suspend.");
        }
        if (PendingHandler) CompleteGates();
        for (var i = 0; i < Pending; i++)
        {
            _processing[i].GetAwaiter().GetResult();
            _directScopes[i].DisposeAsync().GetAwaiter().GetResult(); // No asynchronous disposers in this fixture.
            _directScopes[i] = default;
        }
    }

    [Benchmark]
    public void SettlementWindow()
    {
        ResetGates();
        for (var i = 0; i < Pending; i++)
        {
            _contexts[i] = _pipeline.RentContext(_message);
            _processing[i] = _pipeline.ProcessAsync(_contexts[i]);
            if (_processing[i].IsCompleted)
                throw new InvalidOperationException("The pipeline must genuinely suspend.");
        }
        CompleteGates();
        for (var i = 0; i < Pending; i++)
        {
            _processing[i].GetAwaiter().GetResult(); // All gates completed synchronously above.
            _contexts[i].Dispose();
        }
    }

    private void ResetGates()
    {
        _next = 0;
        _handlerGates.Next = 0;
        for (var i = 0; i < Pending; i++) _gates[i].Reset();
    }

    private void CompleteGates()
    {
        for (var i = 0; i < Pending; i++) _gates[i].Complete();
    }

    [GlobalCleanup] public async Task Cleanup() => await _provider.DisposeAsync();
    [Message("benchmark.settlement")] public sealed class Payload;
    public sealed class Handler(HandlerGates gates) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => gates.Gates is { } pending ? pending[gates.Next++].Wait() : default;
    }
    public sealed class HandlerGates
    {
        internal Gate[]? Gates;
        internal int Next;
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
    internal sealed class Gate : IValueTaskSource
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
