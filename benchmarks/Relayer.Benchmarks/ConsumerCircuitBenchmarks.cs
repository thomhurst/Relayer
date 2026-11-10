using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

[MemoryDiagnoser]
public class ConsumerCircuitBenchmarks
{
    private ConsumerCircuit _circuit = null!;

    [GlobalSetup]
    public void Setup() => _circuit = new ConsumerCircuit(.5, 10, TimeSpan.FromMinutes(1),
        TimeSpan.FromSeconds(5), TimeProvider.System);

    [Benchmark]
    public bool ClosedOutcome()
    {
        _circuit.TryEnter(out var probe);
        return _circuit.Complete(probe, false);
    }

    [GlobalCleanup]
    public void Cleanup() => _circuit.Dispose();
}
