using Relayer.Transports;
using Relayer.Pipeline;
using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

[MemoryDiagnoser]
public class ConsumerCircuitBenchmarks
{
    private ConsumerCircuit _circuit = null!;
    private ConsumerCircuitDelivery _delivery = null!;
    private RelayerInstrumentation _instruments = null!;

    [GlobalSetup]
    public void Setup()
    {
        _circuit = new ConsumerCircuit(.5, 10, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(5), TimeProvider.System);
        _instruments = new();
        _delivery = new(new(), _instruments.CreateEndpoint(new() { System = "kafka", Destination = "benchmark" }),
            static () => { }, TimeProvider.System);
    }

    [Benchmark]
    public bool ClosedOutcome()
    {
        _circuit.TryEnter(out var probe);
        return _circuit.Complete(probe, false);
    }

    [Benchmark]
    public bool SerialDeliveryOutcome()
    {
        _delivery.TryEnter();
        return _delivery.Complete(false);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _delivery.Dispose();
        _instruments.Dispose();
        _circuit.Dispose();
    }
}
