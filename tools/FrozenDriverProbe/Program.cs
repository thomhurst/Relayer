using System.Reflection;
using System.Runtime.Loader;
using System.Text.Json;
using Relayer.Diagnostics;

// Separate instrumented full-delivery control, never a rebuild of historical workload images.
if (args.Length != 2 || args[1] != "--exact-driver")
    throw new ArgumentException("Expected the retained dependency directory and --exact-driver.");
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
// Match the original generated BDN driver's completion boundary using its native helper.
var bdn = context.LoadFromAssemblyPath(Path.Combine(image, "BenchmarkDotNet.dll"));
var helper = bdn.GetType("BenchmarkDotNet.Helpers.AwaitHelper", throwOnError: true)!;
var getResult = helper.GetMethods(BindingFlags.Public | BindingFlags.Static).Single(method =>
    method.Name == "GetResult" && method.IsGenericMethodDefinition && method.GetParameters().Length == 1 &&
    method.GetParameters()[0].ParameterType.IsGenericType &&
    method.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(ValueTask<>));
var wait = getResult.MakeGenericMethod(typeof(int)).CreateDelegate<Func<ValueTask<int>, int>>();
const int warmupBatches = 4096;
const int measuredBatches = 32768;
var starts = new InvocationAllocations(measuredBatches);
var waits = new InvocationAllocations(measuredBatches);
Func<Func<ValueTask<int>>, ValueTask<int>> begin = static callback => callback();
Func<(Func<ValueTask<int>, int> Wait, ValueTask<int> Pending), int> finish = static state => state.Wait(state.Pending);
await setup();
try
{
    for (var i = 0; i < warmupBatches; i++) _ = Batch();
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    starts.Enabled = waits.Enabled = true;
    FrozenMeasurementEvents.Log.MeasurementStart();
    var allocated = GC.GetTotalAllocatedBytes(precise: true);
    long sum = 0;
    for (var i = 0; i < measuredBatches; i++) sum += Batch();
    var bytes = GC.GetTotalAllocatedBytes(precise: true) - allocated;
    FrozenMeasurementEvents.Log.MeasurementStop();
    starts.Enabled = waits.Enabled = false;
    if (sum != measuredBatches * 8L * 42) throw new InvalidOperationException($"Unexpected delivery sum: {sum}.");
    var invocation = starts.Report();
    var completion = waits.Report();
    if (invocation.Invocations != measuredBatches || completion.Invocations != measuredBatches)
        throw new InvalidOperationException("Incomplete driver allocation records.");
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        image, warmupBatches, measuredBatches, messages = measuredBatches * 8,
        bytes, bytesPerMessage = bytes / (measuredBatches * 8.0),
        instrumentation = "exact-driver-calling-thread", invocation, completion,
        runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
        arch = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
        limits = "Exact calling-thread byte counts around the synchronous workload call and original BDN waiter separately. No await between counter reads. Excludes allocations on other threads; waiter includes any inline continuations on the calling thread. Does not identify object types or attribute the historical delta. Recording changes scheduling; never subtract from another experiment."
    }));
}
finally { await cleanup(); }

int Batch()
{
    var pending = starts.Invoke(begin, run);
    return waits.Invoke(finish, (wait, pending));
}
