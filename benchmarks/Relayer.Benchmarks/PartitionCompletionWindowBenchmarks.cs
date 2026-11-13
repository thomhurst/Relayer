using Relayer.Transports;
using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Compares in-order and out-of-order completion of the same bounded two-delivery window.</summary>
[MemoryDiagnoser]
public class PartitionCompletionWindowBenchmarks
{
    private PartitionCompletionWindow<long> _window = null!;
    private long _first = 11;
    private long _second = 43;

    [GlobalSetup]
    public void Setup() => _window = new(64);

    [Benchmark(Baseline = true, OperationsPerInvoke = 2)]
    public long InOrder()
    {
        _window.TryTrack(_first, out var first);
        _window.TryTrack(_second, out var second);
        _window.TryComplete(first, out var firstPosition, out _);
        _window.TryComplete(second, out var secondPosition, out _);
        return firstPosition + secondPosition;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public long OutOfOrder()
    {
        _window.TryTrack(_first, out var first);
        _window.TryTrack(_second, out var second);
        _window.TryComplete(second, out _, out _);
        _window.TryComplete(first, out var position, out var count);
        return position + count;
    }
}
