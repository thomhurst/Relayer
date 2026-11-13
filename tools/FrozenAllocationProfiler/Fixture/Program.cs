using System.Runtime.CompilerServices;

var checkpoints = args.Length > 1 && args[1].StartsWith("--windows", StringComparison.Ordinal);
var automatic = args.Length > 1 && args[1].StartsWith("--", StringComparison.Ordinal) && !checkpoints;
var tail = args.Length > 1 && !automatic && !checkpoints;
var region = tail || automatic ? null : NativeAllocationRegion.Load(checkpoints) ?? throw new InvalidOperationException("Profiler path is required.");
var engine = tail ? FixtureEngine.Log : null;
var count = int.Parse(args[0], System.Globalization.CultureInfo.InvariantCulture);
var values = new object[count];
var second = checkpoints ? new object[count] : [];
// JIT all fixture allocation code before the marker, without counting warmup objects.
Fill(new object[10]);
var start = new ManualResetEventSlim();
var ready = new CountdownEvent(4);
var workers = new Thread[4];
var workerThreads = new ulong[workers.Length];
for (var index = 0; index < workers.Length; index++)
{
    var workerIndex = index;
    var from = count * index / workers.Length;
    var to = count * (index + 1) / workers.Length;
    workers[index] = new Thread(() =>
    {
        workerThreads[workerIndex] = region?.CurrentThread() ?? 0;
        ready.Signal();
        start.Wait();
        FillRange(values, from, to);
    });
    workers[index].Start();
}
ready.Wait();
if (automatic)
{
    if (args[1] == "--twice")
    {
        FixtureMeasurement.MeasureWithGc(start, workers);
        FixtureMeasurement.MeasureWithGc(start, workers);
    }
    else
    {
        try { FixtureMeasurement.MeasureWithGc(start, workers, args[1] == "--throw"); }
        catch (InvalidOperationException) when (args[1] == "--throw") { }
    }
}
else if (engine is not null)
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
if (!automatic)
{
    start.Set();
    foreach (var worker in workers) worker.Join();
}
if (engine is not null)
{
    if (args[1] != "missing-benchmark") engine.BenchmarkStop(FixtureEngine.Benchmark);
    if (args[1] == "extra-event") engine.WorkloadActualStart(8);
    Fill(new object[29]); // After benchmark excluded.
}
else if (!automatic)
{
    if (checkpoints)
    {
        region!.Checkpoint(0, 111);
        Fill(second);
        region.Checkpoint(1, 222);
        if (args[1] != "--windows-missing") region.Checkpoint(2, 333, final: true);
    }
    region!.End();
}
if (region is not null)
    for (var worker = 0; worker < workerThreads.Length; worker++)
        Console.WriteLine($"allocation-worker={worker}; thread={workerThreads[worker]}");
GC.KeepAlive(values);
GC.KeepAlive(second);

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

internal static class FixtureMeasurement
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void MeasureWithGc(ManualResetEventSlim start, Thread[] workers, bool throwAfter = false)
    {
        start.Set();
        foreach (var worker in workers) worker.Join();
        if (throwAfter) throw new InvalidOperationException("Expected boundary unwind");
    }
}
