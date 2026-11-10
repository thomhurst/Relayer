using Relayer.Diagnostics;
using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;

// Investigation tool only: bind once to the original executable; never rebuild its workload.
if (args.Length is < 1 or > 2 || (args.Length == 2 && args[1] != "--exact-roundtrips"))
    throw new ArgumentException("Expected the retained benchmark dependency directory and optional --exact-roundtrips.");
var exactRoundTrips = args.Length == 2;
var image = Path.GetFullPath(args[0]);
var context = new AssemblyLoadContext("Frozen", isCollectible: false);
context.Resolving += (_, name) =>
{
    var path = Path.Combine(image, name.Name + ".dll");
    return File.Exists(path) ? context.LoadFromAssemblyPath(path) : null;
};
var assembly = context.LoadFromAssemblyPath(Path.Combine(image, "Relayer.Benchmarks.dll"));
var type = assembly.GetType("Relayer.Benchmarks.EndToEndBenchmarks", throwOnError: true)!;
var fixture = Activator.CreateInstance(type)!;
var setup = type.GetMethod("Setup")!.CreateDelegate<Func<Task>>(fixture);
var cleanup = type.GetMethod("Cleanup")!.CreateDelegate<Func<Task>>(fixture);
var run = type.GetMethod("ConcurrentRoundTrip")!.CreateDelegate<Func<ValueTask<int>>>(fixture);
const int warmupBatches = 4096;
const int measuredBatches = 32768;
const int messagesPerBatch = 8;
var invocations = exactRoundTrips ? new InvocationAllocations(measuredBatches) : null;
await setup();
try
{
    for (var i = 0; i < warmupBatches; i++) await (invocations is null ? run() : invocations.Invoke(run));
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    long result = 0;
    if (invocations is not null) invocations.Enabled = true;
    // Markers sit outside the inclusive counter; profiles are separate instrumented launches.
    FrozenMeasurementEvents.Log.MeasurementStart();
    var allocated = GC.GetTotalAllocatedBytes(precise: true);
    var start = System.Diagnostics.Stopwatch.GetTimestamp();
    for (var i = 0; i < measuredBatches; i++) result += await (invocations is null ? run() : invocations.Invoke(run));
    var elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(start);
    var bytes = GC.GetTotalAllocatedBytes(precise: true) - allocated;
    FrozenMeasurementEvents.Log.MeasurementStop();
    if (invocations is not null) invocations.Enabled = false;
    var invocationReport = invocations?.Report();
    if (invocationReport is not null && invocationReport.Invocations != measuredBatches)
        throw new InvalidOperationException("Exact round-trip count does not match measured batches.");
    if (result != measuredBatches * messagesPerBatch * 42L)
        throw new InvalidOperationException($"Unexpected delivery sum: {result}.");
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        instrumentation = exactRoundTrips ? "exact-synchronous-roundtrip-calls" : "none",
        exactRegion = "One original ConcurrentRoundTrip delegate call per record (eight messages), before awaiting its returned ValueTask. Calling-thread work only; excludes other threads and later continuations. No type or historical attribution; do not subtract from other controls.",
        invocationAllocations = invocationReport,
        image, warmupBatches, measuredBatches, messages = measuredBatches * messagesPerBatch,
        bytes, bytesPerMessage = bytes / (measuredBatches * (double)messagesPerBatch),
        seconds = elapsed.TotalSeconds,
        runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
        arch = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString()
    }));
}
finally
{
    await cleanup();
}
