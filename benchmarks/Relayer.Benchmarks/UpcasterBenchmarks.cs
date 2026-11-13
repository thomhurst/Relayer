using Relayer.Transports;
using Relayer.Pipeline;
using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Typed value upgrades isolate Relayer's chain cost from serializer and user allocations.</summary>
[MemoryDiagnoser]
public class UpcasterBenchmarks
{
    private ServiceProvider _provider = null!;
    private ConsumePipeline _single = null!;
    private ConsumePipeline _batch = null!;
    private readonly DefaultMessageContext[] _contexts = new DefaultMessageContext[10];

    [Params(1, 2, 3)]
    public int WireVersion { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Handler>();
        services.AddSingleton<BatchHandler>();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.Infrastructure().Upcasters.Add(new FirstUpgrade()).Add(new SecondUpgrade());
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>();
            r.AddTransport<Options, Options, Options>("benchmark", t =>
                t.Consume("versions", c => c.Handler<BatchHandler>().Batch(10, TimeSpan.FromSeconds(1))));
        });
        _provider = services.BuildServiceProvider();
        var handlers = _provider.GetRequiredService<HandlerRegistry>();
        _single = Builder().Handler(handlers.GetHandler<V3, Handler>()).Build(1);
        _batch = Builder().BuildBatch(handlers.GetBatchHandler<V3, BatchHandler>(),
            _provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0]);
    }

    private ConsumePipelineBuilder Builder() => new(_provider.GetRequiredService<IServiceScopeFactory>(),
        _provider.GetRequiredService<MessageTypeRegistry>(), _provider.GetRequiredService<IMessageSerializer>(),
        static (_, error, _) => ValueTask.FromException(error));

    private ConsumeMessage Message() => new() { MessageId = "benchmark", MessageType = new("benchmark.upgrade", WireVersion), Body = default };

    [Benchmark]
    public async ValueTask Single()
    {
        var context = _single.RentContext(Message());
        try { await _single.ProcessAsync(context); }
        finally { context.Dispose(); }
    }

    [Benchmark(OperationsPerInvoke = 10)]
    public async ValueTask Batch()
    {
        for (var i = 0; i < _contexts.Length; i++) _contexts[i] = _batch.RentContext(Message());
        try { await _batch.ProcessBatchAsync(_contexts); }
        finally { foreach (var context in _contexts) context.Dispose(); }
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Message("benchmark.upgrade", Version = 1)] public readonly record struct V1(int Value);
    [Message("benchmark.upgrade", Version = 2)] public readonly record struct V2(long Value);
    [Message("benchmark.upgrade", Version = 3)] public readonly record struct V3(long Value);
    public sealed class FirstUpgrade : IUpcaster<V1, V2> { public V2 Upcast(V1 message) => new(message.Value); }
    public sealed class SecondUpgrade : IUpcaster<V2, V3> { public V3 Upcast(V2 message) => new(message.Value); }
    public sealed class Options;
    public sealed class Handler : IHandler<V3>
    {
        public ValueTask HandleAsync(V3 message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    public sealed class BatchHandler : IBatchHandler<V3>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<V3> batch, CancellationToken cancellationToken) => new(BatchResult.Success);
    }
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "benchmark/value";
        // Default value structs avoid allocations and boxing for every requested contract.
        public T Deserialize<T>(ReadOnlySequence<byte> source) => default!;
        public void Serialize<T>(T message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
}
