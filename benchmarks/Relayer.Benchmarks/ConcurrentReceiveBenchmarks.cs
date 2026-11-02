using BenchmarkDotNet.Attributes;
using Relayer.TestFixtures;

namespace Relayer.Benchmarks;

/// <summary>Eight cached-payload handlers suspend together and complete on alternating dedicated workers.</summary>
/// <remarks>All methods include fresh DI scopes and identical scheduling. Run returns inclusive driver plus worker bytes.</remarks>
[MemoryDiagnoser]
public class ConcurrentReceiveBenchmarks
{
    private ConcurrentReceiveFixture _fixture = null!;
    [GlobalSetup] public void Setup() => _fixture = new();
    [Benchmark(Baseline = true, OperationsPerInvoke = ConcurrentReceiveFixture.Count)]
    public long DirectScoped() => _fixture.Run(ConcurrentReceiveFixture.Mode.Scoped);
    [Benchmark(OperationsPerInvoke = ConcurrentReceiveFixture.Count)]
    public long DirectConsumer() => _fixture.Run(ConcurrentReceiveFixture.Mode.Consumer);
    [Benchmark(OperationsPerInvoke = ConcurrentReceiveFixture.Count)]
    public long NativeReceive() => _fixture.Run(ConcurrentReceiveFixture.Mode.Receive);
    [GlobalCleanup] public void Cleanup() => _fixture.Dispose();
}
