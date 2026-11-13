using System.Runtime.CompilerServices;

var tail = args.Length > 1;
var region = tail ? null : NativeAllocationRegion.Load() ?? throw new InvalidOperationException("Profiler path is required.");
var engine = tail ? FixtureEngine.Log : null;
var count = int.Parse(args[0], System.Globalization.CultureInfo.InvariantCulture);
var values = new object[count];
// JIT all fixture allocation code before the marker, without counting warmup objects.
Fill(new object[10]);
var start = new ManualResetEventSlim();
var ready = new CountdownEvent(4);
var workers = new Thread[4];
for (var index = 0; index < workers.Length; index++)
{
    var from = count * index / workers.Length;
    var to = count * (index + 1) / workers.Length;
    workers[index] = new Thread(() =>
    {
        ready.Signal();
        start.Wait();
        FillRange(values, from, to);
    });
    workers[index].Start();
}
ready.Wait();
if (engine is not null)
{
    engine.WorkloadJittingStart(8);
    engine.WorkloadJittingStop(8);
    engine.BenchmarkStart(FixtureEngine.Benchmark);
    engine.WorkloadActualStart(8);
    Fill(new object[17]); // Timed workload excluded.
    engine.WorkloadActualStop(8);
    Fill(new object[19]); // Earlier tail excluded after next stop.
    engine.WorkloadActualStart(8);
    Fill(new object[23]);
    if (args[1] != "missing-stop") engine.WorkloadActualStop(8);
}
else region!.Begin();
start.Set();
foreach (var worker in workers) worker.Join();
if (engine is not null)
{
    if (args[1] != "missing-benchmark") engine.BenchmarkStop(FixtureEngine.Benchmark);
    if (args[1] == "extra-event") engine.WorkloadActualStart(8);
    Fill(new object[29]); // After benchmark excluded.
}
else region!.End();
GC.KeepAlive(values);

[MethodImpl(MethodImplOptions.NoInlining)]
static void Fill(object[] values)
{
    FillRange(values, 0, values.Length);
}

[MethodImpl(MethodImplOptions.NoInlining)]
static void FillRange(object[] values, int from, int to)
{
    for (var i = from; i < to; i++) values[i] = new FixtureTypes.Box<FixtureMessage>(new FixtureMessage(i));
}

internal sealed class FixtureMessage(int value)
{
    internal int Value { get; } = value;
}

internal static class FixtureTypes
{
    internal sealed class Box<T>(T value)
    {
        internal T Value { get; } = value;
    }
}

[System.Diagnostics.Tracing.EventSource(Name = "BenchmarkDotNet.EngineEventSource")]
internal sealed class FixtureEngine : System.Diagnostics.Tracing.EventSource
{
    internal const string Benchmark = "Relayer.Benchmarks.EndToEndBenchmarks.ConcurrentRoundTrip";
    internal static readonly FixtureEngine Log = new();
    [System.Diagnostics.Tracing.Event(1)] public void BenchmarkStart(string benchmark) => WriteEvent(1, benchmark);
    [System.Diagnostics.Tracing.Event(2)] public void BenchmarkStop(string benchmark) => WriteEvent(2, benchmark);
    [System.Diagnostics.Tracing.Event(5)] public void WorkloadJittingStart(long operations) => WriteEvent(5, operations);
    [System.Diagnostics.Tracing.Event(6)] public void WorkloadJittingStop(long operations) => WriteEvent(6, operations);
    [System.Diagnostics.Tracing.Event(15)] public void WorkloadActualStart(long operations) => WriteEvent(15, operations);
    [System.Diagnostics.Tracing.Event(16)] public void WorkloadActualStop(long operations) => WriteEvent(16, operations);
}
