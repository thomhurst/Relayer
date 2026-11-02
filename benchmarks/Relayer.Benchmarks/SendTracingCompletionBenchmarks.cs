using BenchmarkDotNet.Attributes;
using Relayer.TestFixtures;

namespace Relayer.Benchmarks;

/// <summary>Compares suspended native and traced sends completed on another thread after a 128-send burst.</summary>
[MemoryDiagnoser]
public class SendTracingCompletionBenchmarks
{
    private SendTracingCompletionFixture _fixture = null!;

    [Params(false, true)]
    public bool Batch { get; set; }

    [Params(false, true)]
    public bool Listener { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _fixture = new(Batch, Listener);
        for (var i = 0; i < 16; i++) _fixture.Run(traced: true);
    }

    // One operation is one native call, including one whole batch when Batch is true.
    [Benchmark(Baseline = true, OperationsPerInvoke = SendTracingCompletionFixture.Count)]
    public long Native() => _fixture.Run(traced: false);

    [Benchmark(OperationsPerInvoke = SendTracingCompletionFixture.Count)]
    public long Traced() => _fixture.Run(traced: true);

    [GlobalCleanup]
    public void Cleanup() => _fixture.Dispose();
}
