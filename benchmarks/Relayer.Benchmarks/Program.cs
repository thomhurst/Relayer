using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Running;

// Collect allocation stacks in separate profiler executions. Their timings are diagnostic;
// the ordinary native comparisons remain the acceptance measurements.
var config = Environment.GetEnvironmentVariable("RELAYER_NATS_ALLOCATION_PROFILE") == "1"
    ? ManualConfig.Create(DefaultConfig.Instance).AddDiagnoser(new EventPipeProfiler(EventPipeProfile.GcVerbose))
    : null;
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
