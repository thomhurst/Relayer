using System.Runtime.CompilerServices;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Compares direct and scheduled native dispatch with one or several independent partitions.</summary>
[MemoryDiagnoser]
public class InMemorySchedulingBenchmarks
{
    private readonly DispatchClock _clock = new();
    private readonly Channel<int> _received = Channel.CreateBounded<int>(3);
    private readonly byte[] _body = [42];
    private readonly OutgoingMessage[] _batch = new OutgoingMessage[3];
    private InMemoryTransport _transport = null!;

    [Params(1, 4)]
    public int Partitions { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _transport = new("native", new Dictionary<string, InMemoryEndpointOptions>
        {
            ["orders"] = new()
            {
                Partitions = Partitions,
                Capacity = 4,
                MessagePoolCapacity = 4,
                Capabilities = new() { OrderedByKey = true, MaxBatchSize = 3, MaxDeliveryDelay = TimeSpan.FromMinutes(1) },
            },
        }, _clock);
        _transport.RegisterConsumer("orders", (delivery, _) =>
        {
            _received.Writer.TryWrite(delivery.Message.Body.Span[0]);
            return new(Disposition.Complete);
        }, concurrency: Partitions);
        await _transport.StartAsync(default);
    }

    [Benchmark(Baseline = true)]
    public ValueTask<int> DirectRoundTrip() => RoundTrip(false);

    [Benchmark]
    public ValueTask<int> ScheduledRoundTrip() => RoundTrip(true);

    // One invocation is one ordered batch, including heap comparisons for equal due times.
    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> EqualDueBatchRoundTrip()
    {
        var at = _clock.GetUtcNow().AddTicks(1);
        for (var i = 0; i < _batch.Length; i++) _batch[i] = new("orders", _body, orderingKey: new WireString("key"), deliverAt: at);
        await _transport.SendBatchAsync(_batch, default);
        _clock.Advance();
        var total = 0;
        for (var i = 0; i < _batch.Length; i++) total += await _received.Reader.ReadAsync();
        return total;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> RoundTrip(bool scheduled)
    {
        // Enqueue in the future, then make it due without measuring an intentional delivery delay.
        // If the scheduler reads first, its one-tick system timer wakes it after the clock advances.
        await _transport.SendAsync(new("orders", _body,
            deliverAt: scheduled ? _clock.GetUtcNow().AddTicks(1) : null), default);
        _clock.Advance();
        return await _received.Reader.ReadAsync();
    }

    [GlobalCleanup]
    public async Task Cleanup() => await _transport.DisposeAsync();

    private sealed class DispatchClock : TimeProvider
    {
        private long _ticks = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).Ticks;
        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);
        internal void Advance() => Interlocked.Increment(ref _ticks);
    }
}
