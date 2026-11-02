using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Native receive adaptation versus the same composed consumer without the transport seam.</summary>
/// <remarks>Both include identical DI scope allocations and cached deserialization. Their allocation difference is receive SPI cost.</remarks>
[MemoryDiagnoser]
public class ReceivePipelineBenchmarks
{
    private readonly Serializer _serializer = new();
    private ServiceProvider _provider = null!;
    private ConsumePipeline _consumer = null!;
    private ReceivePipeline<Incoming, Binding> _receive = null!;
    private ConsumeMessage _message;
    private Incoming _incoming = null!;
    private Incoming[] _batch = null!;
    private Incoming _tombstone = null!;
    private readonly Disposition[] _dispositions = new Disposition[10];

    [Params(false, true)]
    public bool Utf8Headers { get; set; }

    [Params(false, true)]
    public bool Tenancy { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        if (Tenancy) services.AddRelayerTenancy();
        services.AddSingleton<Handler>();
        services.AddSingleton<TombstoneHandler>();
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("benchmark.payload"); r.Handler<Handler>().Handler<TombstoneHandler>(); });
        _provider = services.BuildServiceProvider();
        _consumer = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
                _provider.GetRequiredService<MessageTypeRegistry>(), _serializer,
                static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>())
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Tombstone<Payload>, TombstoneHandler>()).Build(1);
        _receive = new(_consumer, new HeaderNames(), 1);
        _incoming = new Incoming(Utf8Headers ? new WireString("benchmark.payload;v=1"u8.ToArray()) : new("benchmark.payload;v=1"));
        _tombstone = new Incoming(_incoming.Type, tombstone: true);
        _batch = new Incoming[10];
        Array.Fill(_batch, _incoming);
        _message = new ConsumeMessage
        {
            MessageId = "benchmark", MessageType = new("benchmark.payload", 1), Body = _incoming.Body,
        };
    }

    // Equivalent scope/service/serializer work attributes DI allocations separately from the receive SPI.
    [Benchmark]
    public Payload NativeScope()
    {
        using var scope = _provider.CreateScope();
        if (Tenancy) scope.ServiceProvider.GetRequiredService<TenantContext>().Id = _incoming.Tenant;
        _ = scope.ServiceProvider.GetRequiredService<Handler>();
        return _serializer.Deserialize<Payload>(_incoming.Body);
    }

    [Benchmark(Baseline = true)]
    public async ValueTask DirectConsumer()
    {
        using var context = _consumer.RentContext(_message);
        await _consumer.ProcessAsync(context).ConfigureAwait(false);
    }

    [Benchmark]
    public ValueTask<Disposition> NativeReceive() => _receive.ProcessAsync(_incoming);

    [Benchmark]
    public ValueTask<Disposition> NativeTombstone() => _receive.ProcessAsync(_tombstone);

    [Benchmark(OperationsPerInvoke = 10)]
    public ValueTask NativeBatch() => _receive.ProcessBatchAsync(_batch, _dispositions);

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Message("benchmark.payload")]
    public sealed class Payload;
    public sealed class Handler : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    public sealed class TombstoneHandler : IHandler<Tombstone<Payload>>
    {
        public ValueTask HandleAsync(Tombstone<Payload> message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    private sealed class Incoming(WireString type, bool tombstone = false)
    {
        public WireString Type { get; } = type;
        public WireString Tenant { get; } = new("tenant-1"u8.ToArray());
        public bool Tombstone { get; } = tombstone;
        public ReadOnlySequence<byte> Body { get; } = tombstone ? ReadOnlySequence<byte>.Empty : new(new byte[64]);
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => message.Body;
        public static bool IsTombstone(Incoming message) => message.Tombstone;
        public static WireString? GetTombstoneKey(Incoming message) => new WireString("customer-1");
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch
            {
                "ce_type" => message.Type,
                "ce_id" => new WireString("benchmark"),
                TenantContext.HeaderName => message.Tenant,
                _ => default,
            };
            return !value.IsEmpty;
        }
    }
}
