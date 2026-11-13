using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Toolchains.DotNetCli;
using Relayer.TestFixtures;

namespace Relayer.Benchmarks;

/// <summary>
/// Tests the completion mechanism shared by consume, receive, send, tracing and saga operations.
/// This is a lower-bound builder experiment, not a throughput comparison of complete pipelines.
/// Dedicated worker allocations are included in the returned byte count; MemoryDiagnoser alone
/// observes the invoking thread. All variants hold the whole burst until completion and GetResult.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(Frameworks))]
public class PooledBuilderCompletionBenchmarks
{
    // BenchmarkDotNet 0.15.8 has no Net11 runtime enum; select its target toolchain explicitly.
    public sealed class Frameworks : ManualConfig
    {
        public Frameworks()
        {
            AddJob(Job.Default.WithRuntime(CoreRuntime.Core10_0).WithId("net10.0"));
            // 0.15.8 also switches on its enum during validation and partitioning, so a
            // CreateForNewVersion runtime cannot run. Use the known moniker for that metadata
            // only; the toolchain and MSBuild argument target net11.0. The job ID identifies
            // the target; verify the actual .NET 11 process header in the exported log.
            AddJob(Job.Default.WithRuntime(CoreRuntime.Core10_0)
                .WithToolchain(CsProjCoreToolchain.From(new NetCoreAppSettings("net11.0", null, ".NET 11.0")))
                .WithArguments([new MsBuildArgument("/p:RelayerBenchmarkTargetFramework=net11.0")])
                .WithId("net11.0"));
        }
    }

    private PooledBuilderCompletionFixture _fixture = null!;
    [Params(1, 64)] public int Concurrency { get; set; }
    [Params(false, true)] public bool Asynchronous { get; set; }
    [Params(false, true)] public bool AmbientContext { get; set; }
    [GlobalSetup] public void Setup() => _fixture = new(Concurrency, Asynchronous, AmbientContext);
    [Benchmark(Baseline = true)] public long Native() => _fixture.Run(PooledBuilderCompletionFixture.Mode.Native);
    [Benchmark] public long OwnedCompletion() => _fixture.Run(PooledBuilderCompletionFixture.Mode.Owned);
    [Benchmark] public long PooledVoid() => _fixture.Run(PooledBuilderCompletionFixture.Mode.PooledVoid);
    [Benchmark] public long PooledResult() => _fixture.Run(PooledBuilderCompletionFixture.Mode.PooledResult);
    [GlobalCleanup] public void Cleanup() => _fixture.Dispose();
}
