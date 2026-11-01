using System.Runtime.CompilerServices;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Measures native queue ownership and dispatch with 64 messages simultaneously in flight.</summary>
[MemoryDiagnoser]
public class InMemoryQueuedBatchBenchmarks
{
    private readonly OutgoingMessage[] _batch = new OutgoingMessage[64];
    private readonly Channel<byte> _release = Channel.CreateBounded<byte>(1);
    private readonly Channel<int> _received = Channel.CreateBounded<int>(1);
    private InMemoryTransport _transport = null!;
    private int _count;

    [GlobalSetup]
    public async Task Setup()
    {
        var body = new byte[] { 42 };
        for (var i = 0; i < _batch.Length; i++) _batch[i] = new("orders", body);
        _transport = new("native", new Dictionary<string, InMemoryEndpointOptions>
        {
            ["orders"] = new() { Capacity = 64, MessagePoolCapacity = 64, Capabilities = new() { MaxBatchSize = 64 } },
        });
        _transport.RegisterConsumer("orders", Receive);
        await _transport.StartAsync(default);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Disposition> Receive(InMemoryDelivery delivery, CancellationToken cancellationToken)
    {
        // Hold the first delivery until the whole batch is accepted, exercising all 64 pool slots.
        if (_count == 0) await _release.Reader.ReadAsync();
        if (++_count == _batch.Length)
        {
            _count = 0;
            _received.Writer.TryWrite(_batch.Length);
        }
        return Disposition.Complete;
    }

    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> QueuedBatchRoundTrip()
    {
        await _transport.SendBatchAsync(_batch, default);
        _release.Writer.TryWrite(0);
        return await _received.Reader.ReadAsync();
    }

    [GlobalCleanup]
    public async Task Cleanup() => await _transport.DisposeAsync();
}
