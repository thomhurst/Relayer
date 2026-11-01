#pragma warning disable RLY001 // Explicit benchmark wire identity and cached serializer.
using System.Buffers;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Compares identical native delivery and DI work with process and settlement tracing enabled.</summary>
[MemoryDiagnoser]
public class ConsumeTracingBenchmarks
{
    private ServiceProvider _provider = null!;
    private MessageTracing _tracing = null!;
    private ActivityListener? _listener;
    private ReceivePipeline<Incoming, Binding> _baseline = null!;
    private ReceivePipeline<Incoming, Binding> _traced = null!;
    private readonly Incoming _incoming = new();

    [Params("NoListener", "Drop", "Record")]
    public string Mode { get; set; } = "NoListener";

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Handler>();
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("benchmark.payload"); r.Handler<Handler>(); });
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
    }

    private ConsumePipelineBuilder Builder() => new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
        .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>());

    [Benchmark(Baseline = true)]
    public ValueTask<Disposition> NativeDelivery() => _baseline.ProcessAsync(_incoming);

    [Benchmark]
    public ValueTask<Disposition> TracedNativeDelivery() => _traced.ProcessAsync(_incoming);

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
