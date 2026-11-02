using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>
/// Isolates pipeline cost and allocations with a synchronous sink. The baseline serializes and calls the sink;
/// it excludes the envelope, routing, middleware and timeout. This is not a broker throughput comparison.
/// </summary>
[MemoryDiagnoser]
public class SendPipelineBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private ISender _sender = null!;
    private IMessageSerializer _serializer = null!;
    private readonly SinkTransport _transport = new();
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly SendOrder _message = new(42);

    [Params(false, true)]
    public bool Middleware { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ITransport>(_transport);
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AddTransport<SendBenchmarkOptions, SendBenchmarkOptions, SendBenchmarkOptions>("sink",
                transport => transport.Send<SendOrder>("orders", route => route.OrderingKey(static message => message.Id)));
            if (Middleware) builder.UseSendMiddleware<PassThroughMiddleware>();
        });
        _provider = services.BuildServiceProvider();
        _scope = _provider.CreateScope();
        _sender = _scope.ServiceProvider.GetRequiredService<ISender>();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
    }

    [Benchmark(Baseline = true)]
    public ValueTask SerializeAndCallNative()
    {
        _buffer.Clear();
        _serializer.Serialize(_message, _buffer);
        return _transport.SendAsync(new OutgoingMessage("orders", _buffer.WrittenMemory), default);
    }

    [Benchmark]
    public ValueTask TombstoneSend() => _sender.SendAsync(Tombstone.For<SendOrder>("customer-1"));

    [Benchmark]
    public ValueTask DirectSend() => _sender.SendAsync(_message);

    [GlobalCleanup]
    public void Cleanup()
    {
        _scope.Dispose();
        _provider.Dispose();
    }

    public sealed class PassThroughMiddleware : ISendMiddleware
    {
        public ValueTask InvokeAsync(SendContext context, SendDelegate next) => next(context);
    }

    private sealed class SinkTransport : ITransport
    {
        public string Name => "sink";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true, Tombstones = true };
        public int Bytes { get; private set; }
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Bytes = message.Body.Length;
            return ValueTask.CompletedTask;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
            => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

[Message("bench.send-order")]
public sealed record SendOrder(int Id);
public sealed class SendBenchmarkOptions;
