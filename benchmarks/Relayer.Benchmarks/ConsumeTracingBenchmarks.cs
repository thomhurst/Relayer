using Relayer.Transports;
using Relayer.Pipeline;
#pragma warning disable RLY001 // Explicit benchmark wire identity and cached serializer.
using System.Buffers;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Compares identical native delivery and DI work with process and settlement tracing enabled.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ConsumeTracingBenchmarks
{
    private ServiceProvider _provider = null!;
    private MessageTracing _tracing = null!;
    private ActivityListener? _listener;
    private ReceivePipeline<Incoming, Binding> _baseline = null!;
    private ReceivePipeline<Incoming, Binding> _traced = null!;
    private ReceivePipeline<Incoming, Binding> _batchBaseline = null!;
    private ReceivePipeline<Incoming, Binding> _batchTraced = null!;
    private readonly Incoming[] _batch = new Incoming[10];
    private readonly Disposition[] _dispositions = new Disposition[10];
    private readonly Incoming _incoming = new();

    [Params("NoListener", "Drop", "Record")]
    public string Mode { get; set; } = "NoListener";

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Handler>();
        services.AddSingleton<BatchHandler>();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddRelayer(r =>
        {
            r.Infrastructure().MessageTypes.Add<Payload>("benchmark.payload");
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>();
            r.AddTransport<BatchOptions, BatchOptions>("benchmark", transport =>
                transport.Consume("orders", consumer => consumer.Handler<BatchHandler>().Batch(10, TimeSpan.FromMilliseconds(100))));
        });
        _provider = services.BuildServiceProvider();
        _tracing = new(Options.Create(new TracingOptions()), []);
        if (Mode != "NoListener")
        {
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, _tracing.Source),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                    Mode == "Drop" ? ActivitySamplingResult.None : ActivitySamplingResult.AllDataAndRecorded,
            };
            ActivitySource.AddActivityListener(_listener);
        }
        var endpoint = new MessagingEndpoint { System = "kafka", Destination = "orders", ConsumerGroup = "billing" };
        _baseline = new(Builder().Settle(static _ => default).Build(1), new(), 1);
        var complete = new MessageOperationTracing(_tracing, endpoint, MessagingOperation.Settle, "commit").WrapSettlement(static _ => default);
        _traced = new(Builder().Tracing(_tracing, endpoint).Settle(complete).Build(1), new(), 1);
        var definition = _provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        var descriptor = _provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, BatchHandler>();
        _batchBaseline = new(BatchBuilder().Settle(static _ => default).BuildBatch(descriptor, definition), new(), 10);
        _batchTraced = new(BatchBuilder().Tracing(_tracing, endpoint).Settle(complete).BuildBatch(descriptor, definition), new(), 10);
        Array.Fill(_batch, _incoming);
    }

    private ConsumePipelineBuilder Builder() => new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
        .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>());

    private ConsumePipelineBuilder BatchBuilder() => new(_provider.GetRequiredService<IServiceScopeFactory>(),
        _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error));

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Single")]
    public ValueTask<Disposition> NativeDelivery() => _baseline.ProcessAsync(_incoming);

    [Benchmark]
    [BenchmarkCategory("Single")]
    public ValueTask<Disposition> TracedNativeDelivery() => _traced.ProcessAsync(_incoming);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Batch")]
    public ValueTask NativeBatch() => _batchBaseline.ProcessBatchAsync(_batch, _dispositions);

    [Benchmark]
    [BenchmarkCategory("Batch")]
    public ValueTask TracedNativeBatch() => _batchTraced.ProcessBatchAsync(_batch, _dispositions);

    [GlobalCleanup]
    public void Cleanup()
    {
        _listener?.Dispose();
        _tracing.Dispose();
        _provider.Dispose();
    }

    private sealed class Incoming
    {
        internal byte[] TraceParent { get; } = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"u8.ToArray();
        internal byte[] Type { get; } = "benchmark.payload;v=1"u8.ToArray();
        internal byte[] Id { get; } = "message"u8.ToArray();
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => default;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static string? GetPartitionId(Incoming message) => "3";
        public static void EnrichActivity(Activity activity, Incoming message) => activity.SetTag("messaging.kafka.offset", 42L);
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch
            {
                "traceparent" => new(message.TraceParent), "ce_type" => new(message.Type), "ce_id" => new(message.Id),
                _ => default,
            };
            return !value.IsEmpty;
        }
    }
    public sealed class Payload;
    public sealed class BatchOptions;
    public sealed class BatchHandler : IBatchHandler<Payload>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<Payload> batch, CancellationToken cancellationToken) => new(BatchResult.Success);
    }
    public sealed class Handler : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public T Deserialize<T>(ReadOnlySequence<byte> source) => (T)(object)_payload;
        public void Serialize<T>(T message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
}
