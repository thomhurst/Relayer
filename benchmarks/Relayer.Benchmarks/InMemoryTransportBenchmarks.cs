using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Compares a serialized native round trip with the same adapter plus generated core receive dispatch.</summary>
/// <remarks>Includes native buffer ownership and scheduling, JSON serialization, and an acknowledgement channel in both paths.</remarks>
[MemoryDiagnoser]
public class InMemoryTransportBenchmarks
{
    private InMemoryTransport _native = null!;
    private InMemoryTransport _composed = null!;
    private InMemoryTransport _opaque = null!;
    private OutgoingMessage _opaqueMessage;
    private ServiceProvider _provider = null!;
    private IMessageSerializer _serializer = null!;
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly Channel<int> _received = Channel.CreateBounded<int>(1);
    private readonly InMemoryBenchmarkOrder _message = new(42);
    private readonly KeyValuePair<string, WireString>[] _headers =
    [
        new("ce_id", new WireString("benchmark")),
        new("ce_type", new WireString("bench.inmemory-order;v=1")),
        new("traceparent", new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")),
    ];

    [Params(ConsumptionModel.Queue, ConsumptionModel.Log)]
    public ConsumptionModel Mode { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton(_received);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddHandlersFromThisAssembly();
        });
        _provider = services.BuildServiceProvider();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
        var endpoints = new Dictionary<string, InMemoryEndpointOptions>
        {
            ["orders"] = new() { Capacity = 4, MessagePoolCapacity = 4, Capabilities = new() { ConsumptionModel = Mode } },
        };
        _native = new("native", endpoints);
        _composed = new("composed", endpoints);
        _opaque = new("opaque", endpoints);
        _serializer.Serialize(_message, _buffer);
        _opaqueMessage = new("orders", _buffer.WrittenMemory, _headers);
        var group = Mode == ConsumptionModel.Log ? "benchmark" : null;
        _opaque.RegisterConsumer("orders", (delivery, _) =>
        {
            _received.Writer.TryWrite(delivery.Message.Body.Length);
            return new(Disposition.Complete);
        }, group);
        _native.RegisterConsumer("orders", (delivery, _) =>
        {
            var message = _serializer.Deserialize<InMemoryBenchmarkOrder>(new ReadOnlySequence<byte>(delivery.Message.Body));
            _received.Writer.TryWrite(message.Id);
            return new(Disposition.Complete);
        }, group);
        var consumer = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), _serializer, static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<InMemoryBenchmarkOrder, InMemoryBenchmarkHandler>()).Build(1);
        var receive = new ReceivePipeline<InMemoryDelivery, InMemoryMessageBinding>(consumer, new HeaderNames(), 1);
        _composed.RegisterConsumer("orders", receive.ProcessAsync, group);
        await _native.StartAsync(default);
        await _composed.StartAsync(default);
        await _opaque.StartAsync(default);
    }

    // Isolates native adapter ownership, channels and settlement without serializer allocations.
    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> NativeOpaqueRoundTrip()
    {
        await _opaque.SendAsync(_opaqueMessage, default);
        return await _received.Reader.ReadAsync();
    }

    [Benchmark(Baseline = true)]
    public ValueTask<int> NativeRoundTrip() => RoundTrip(_native);

    [Benchmark]
    public ValueTask<int> ComposedRoundTrip() => RoundTrip(_composed);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> RoundTrip(InMemoryTransport transport)
    {
        _buffer.Clear();
        _serializer.Serialize(_message, _buffer);
        await transport.SendAsync(new("orders", _buffer.WrittenMemory, _headers), default);
        return await _received.Reader.ReadAsync();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _native.DisposeAsync();
        await _composed.DisposeAsync();
        await _opaque.DisposeAsync();
        await _provider.DisposeAsync();
    }
}

[Message("bench.inmemory-order")]
public readonly record struct InMemoryBenchmarkOrder(int Id);
public sealed class InMemoryBenchmarkHandler(Channel<int> received) : IHandler<InMemoryBenchmarkOrder>
{
    public ValueTask HandleAsync(InMemoryBenchmarkOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        received.Writer.TryWrite(message.Id);
        return default;
    }
}
