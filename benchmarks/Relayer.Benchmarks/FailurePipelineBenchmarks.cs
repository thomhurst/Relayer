using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Successful dispatch with and without failure handling, including a handler's scoped sender dependency.</summary>
/// <remarks>Cached serialization isolates pipeline cost. All cases use the same native DI scope and sender pool.</remarks>
[MemoryDiagnoser]
public class FailurePipelineBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScopeFactory _scopes = null!;
    private ConsumePipeline _consumer = null!;
    private ConsumePipeline _failures = null!;
    private HandlerDescriptor<Payload> _descriptor = null!;
    private readonly DirectContext _direct = new();
    private readonly Serializer _serializer = new();
    private ConsumeMessage _message;

    [Params(false, true)]
    public bool SendMessage { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport, Transport>();
        services.AddSingleton<IMessageSerializer>(_serializer);
        services.AddSingleton(this);
        services.AddRelayer(r =>
        {
            r.MessageTypes.Add<Payload>("benchmark.failure");
            r.Handler<Handler>();
            r.AddTransport<Options, Options, Options>("benchmark", t => t.Send<Payload>("destination"));
        });
        _provider = services.BuildServiceProvider();
        _scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        _descriptor = _provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>();
        var policy = new FailurePipeline(_provider, "benchmark", "source", "consumer");
        _consumer = new ConsumePipelineBuilder(_scopes, _provider.GetRequiredService<MessageTypeRegistry>(), _serializer,
                static (_, error, _) => ValueTask.FromException(error)).Handler(_descriptor).Build(1);
        _failures = new ConsumePipelineBuilder(_scopes, _provider.GetRequiredService<MessageTypeRegistry>(), _serializer,
                policy).Handler(_descriptor).Build(1);
        _message = new() { MessageId = "benchmark", MessageType = new("benchmark.failure", 1), Body = default };
    }

    [Benchmark(Baseline = true)]
    public async ValueTask DirectScopedDispatch()
    {
        var scope = new AsyncServiceScope(_scopes.CreateScope());
        try
        {
            _direct.Provider = scope.ServiceProvider;
            var handler = _descriptor.Resolve(scope.ServiceProvider);
            await _descriptor.DispatchAsync(handler, _serializer.Deserialize<Payload>(default), _direct, default).ConfigureAwait(false);
        }
        finally { await scope.DisposeAsync().ConfigureAwait(false); }
    }

    [Benchmark]
    public async ValueTask ExplicitFailureCallback()
    {
        using var context = _consumer.RentContext(_message);
        await _consumer.ProcessAsync(context).ConfigureAwait(false);
    }

    [Benchmark]
    public async ValueTask ExecutableFailurePipeline()
    {
        using var context = _failures.RentContext(_message);
        await _failures.ProcessAsync(context).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup() => await _provider.DisposeAsync();

    [Message("benchmark.failure")]
    public sealed class Payload;
    public sealed class Handler(ISender sender, FailurePipelineBenchmarks settings) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
            => settings.SendMessage ? sender.SendAsync(message, cancellationToken) : default;
    }
    public sealed class Options;
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            "{}"u8.CopyTo(destination.GetSpan(2));
            destination.Advance(2);
        }
    }
    private sealed class Transport : ITransport
    {
        public string Name => "benchmark";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { DeadLetter = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => default;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => default;
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
            => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
    private sealed class DirectContext : MessageContext
    {
        internal IServiceProvider Provider { get; set; } = null!;
        public override string MessageId => "benchmark";
        public override MessageType MessageType => new("benchmark.failure", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => Provider;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
