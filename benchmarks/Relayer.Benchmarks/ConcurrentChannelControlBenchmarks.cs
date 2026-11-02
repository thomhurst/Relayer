using System.Runtime.CompilerServices;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Isolates the eight-delivery fixture's channels and asynchronous handler from send/receive composition.</summary>
/// <remarks>Results are per item, like EndToEndBenchmarks.ConcurrentRoundTrip. No serializer, DI scopes or transport runs here.</remarks>
[MemoryDiagnoser]
public class ConcurrentChannelControlBenchmarks
{
    private const int ConcurrentMessages = 8;
    private readonly EndToEndSignals _signals = new();
    private readonly ConcurrentOrder _order = new(42);
    private readonly Channel<ConcurrentOrder> _work = Channel.CreateBounded<ConcurrentOrder>(ConcurrentMessages);
    private readonly CancellationTokenSource _limit = new();
    private Task[] _workers = null!;

    [GlobalSetup]
    public void Setup()
    {
        _limit.CancelAfter(TimeSpan.FromMinutes(15));
        var handler = new ConcurrentHandler(_signals);
        _workers = new Task[ConcurrentMessages];
        for (var i = 0; i < _workers.Length; i++) _workers[i] = Task.Run(() => RunWorker(handler));
    }

    private async Task RunWorker(ConcurrentHandler handler)
    {
        try
        {
            while (true)
            {
                var order = await _work.Reader.ReadAsync();
                await handler.HandleAsync(order, null!, _limit.Token);
            }
        }
        catch (ChannelClosedException) { }
    }

    [Benchmark(OperationsPerInvoke = ConcurrentMessages)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ConcurrentChannelControl()
    {
        // Match the gate's eight genuinely pending handler reads and its three signal channels.
        for (var i = 0; i < ConcurrentMessages; i++)
            if (!_work.Writer.TryWrite(_order)) throw new InvalidOperationException("Control channel is full.");
        for (var i = 0; i < ConcurrentMessages; i++) await _signals.Entered.Reader.ReadAsync(_limit.Token);
        for (var i = 0; i < ConcurrentMessages; i++)
            if (!_signals.Release.Writer.TryWrite(0)) throw new InvalidOperationException("Release channel is full.");
        var total = 0;
        for (var i = 0; i < ConcurrentMessages; i++) total += await _signals.Completed.Reader.ReadAsync(_limit.Token);
        return total;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _work.Writer.TryComplete();
        await Task.WhenAll(_workers);
        _limit.Dispose();
    }
}
