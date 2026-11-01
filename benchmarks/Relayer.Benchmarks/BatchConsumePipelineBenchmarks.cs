using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Batch composition versus the same scoped sender, serializer and generated handler dispatch.</summary>
/// <remarks>Results are per batch. DI scope allocations are shared by both paths; payload instances are cached.</remarks>
[MemoryDiagnoser]
public class BatchConsumePipelineBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScopeFactory _scopes = null!;
    private ConsumePipeline _pipeline = null!;
    private BatchHandlerDescriptor<Payload> _descriptor = null!;
    private readonly Serializer _serializer = new();
    private readonly DirectContext _directContext = new();
    private Batch<Payload> _directBatch = null!;
    private ConsumeMessage _message;
    private DefaultMessageContext[] _contexts = null!;

    [Params(1, 10, 100)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageSerializer>(_serializer);
        services.AddSingleton<Handler>();
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Payload>("benchmark.batch");
            builder.AddTransport<Options, Options, Options>("benchmark", transport =>
                transport.Consume("source", consumer => consumer.Handler<Handler>().Batch(BatchSize, TimeSpan.FromMilliseconds(100))));
        });
        _provider = services.BuildServiceProvider();
        _scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        _descriptor = _provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>();
        var definition = _provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        _pipeline = new ConsumePipelineBuilder(_scopes, _provider.GetRequiredService<MessageTypeRegistry>(), _serializer,
            static (_, error, _) => ValueTask.FromException(error)).BuildBatch(_descriptor, definition);
        _message = new() { MessageId = "benchmark", MessageType = new("benchmark.batch", 1), Body = ReadOnlySequence<byte>.Empty };
        _contexts = new DefaultMessageContext[BatchSize];
        var items = new BatchItem<Payload>[BatchSize];
        for (var i = 0; i < items.Length; i++) items[i] = new(_serializer.Deserialize<Payload>(_message.Body), _directContext);
        _directBatch = new(items);
    }

    [Benchmark(Baseline = true)]
    public async ValueTask DirectScopedBatch()
    {
        var scope = new AsyncServiceScope(_scopes.CreateScope());
        try
        {
            _directContext.Provider = scope.ServiceProvider;
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
            for (var i = 0; i < BatchSize; i++) _ = _serializer.Deserialize<Payload>(_message.Body);
            await _descriptor.DispatchAsync(_descriptor.Resolve(scope.ServiceProvider), _directBatch, default).ConfigureAwait(false);
        }
        finally { await scope.DisposeAsync().ConfigureAwait(false); }
    }

    [Benchmark]
    public async ValueTask ComposedBatch()
    {
        for (var i = 0; i < BatchSize; i++) _contexts[i] = _pipeline.RentContext(_message);
        try { await _pipeline.ProcessBatchAsync(_contexts).ConfigureAwait(false); }
        finally
        {
            for (var i = 0; i < BatchSize; i++) { _contexts[i].Dispose(); _contexts[i] = null!; }
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();
    public sealed class Options;
    [Message("benchmark.batch")]
    public sealed class Payload { public int Value => 42; }
    public sealed class Handler : IBatchHandler<Payload>
    {
        public int Total { get; private set; }
        public ValueTask<BatchResult> HandleAsync(Batch<Payload> batch, CancellationToken cancellationToken)
        {
            var total = 0;
            for (var i = 0; i < batch.Count; i++) total += batch.Items[i].Message.Value;
            Total = total;
            return new(BatchResult.Success);
        }
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
        public IServiceProvider Provider { get; set; } = null!;
        public override string MessageId => "benchmark";
        public override MessageType MessageType => new("benchmark.batch", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => Provider;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
