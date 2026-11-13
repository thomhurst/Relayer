using Relayer.Pipeline;
using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Buffered batch sends with shared or distinct cancellable delivery tokens. Results are per batch.</summary>
[MemoryDiagnoser]
public class BatchBufferedSendBenchmarks
{
    private ServiceProvider _provider = null!;
    private ConsumePipeline _pipeline = null!;
    private DefaultMessageContext[] _contexts = null!;
    private CancellationTokenSource[] _deliveries = null!;
    private readonly CancellationTokenSource _batch = new();
    private readonly ConsumeMessage _message = new()
    {
        MessageId = "benchmark", MessageType = new("benchmark.buffered-batch", 1), Body = ReadOnlySequence<byte>.Empty
    };

    [Params(1, 10, 100)]
    public int BatchSize { get; set; }

    [Params(false, true)]
    public bool DistinctDeliveryTokens { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddSingleton<ITransport, Transport>();
        services.AddSingleton<Handler>();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Payload>("benchmark.buffered-batch");
            builder.AddTransport<Options, Options>("benchmark", transport =>
            {
                transport.Send<Payload>("destination");
                transport.Consume("source", consumer => consumer.Handler<Handler>().Batch(BatchSize, TimeSpan.FromSeconds(1)));
            });
        });
        _provider = services.BuildServiceProvider();
        _pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _provider.GetRequiredService<IMessageSerializer>(),
            static (_, error, _) => ValueTask.FromException(error))
            .BuildBatch(_provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>(),
                _provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0]);
        _contexts = new DefaultMessageContext[BatchSize];
        _deliveries = new CancellationTokenSource[DistinctDeliveryTokens ? BatchSize : 1];
        for (var i = 0; i < _deliveries.Length; i++) _deliveries[i] = new();
    }

    [Benchmark]
    public async ValueTask BufferedBatch()
    {
        for (var i = 0; i < BatchSize; i++)
            _contexts[i] = _pipeline.RentContext(_message, _deliveries[DistinctDeliveryTokens ? i : 0].Token);
        try { await _pipeline.ProcessBatchAsync(_contexts, _batch.Token).ConfigureAwait(false); }
        finally
        {
            for (var i = 0; i < BatchSize; i++) { _contexts[i].Dispose(); _contexts[i] = null!; }
        }
    }

    [GlobalCleanup]
    public async ValueTask Cleanup()
    {
        foreach (var delivery in _deliveries) delivery.Dispose();
        _batch.Dispose();
        await _provider.DisposeAsync().ConfigureAwait(false);
    }

    public sealed class Options;
    [Message("benchmark.buffered-batch")]
    public sealed class Payload;
    public sealed class Handler : IBatchHandler<Payload>
    {
        public async ValueTask<BatchResult> HandleAsync(Batch<Payload> batch, CancellationToken cancellationToken)
        {
            var sender = batch.Items[0].Context.Services.GetRequiredService<ISender>();
            for (var i = 0; i < batch.Count; i++)
                await sender.SendNowAsync(batch.Items[i].Message, cancellationToken).ConfigureAwait(false);
            return BatchResult.Success;
        }
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) { destination.GetSpan(1)[0] = 1; destination.Advance(1); }
    }
    private sealed class Transport : ITransport
    {
        public string Name => "benchmark";
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => default;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
