using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text.Json;
using System.Threading.Channels;
using Relayer.Diagnostics;

// Diagnostic only. Bind the original handler once; never rebuild historical workload assemblies.
if (args.Length is < 1 or > 2 || (args.Length == 2 && args[1] is not ("--exact-invocations" or "--exact-releases" or "--exact-sources")))
    throw new ArgumentException("Expected the retained benchmark dependency directory and optional --exact-invocations, --exact-releases or --exact-sources.");
var exactInvocations = args.Length == 2 && args[1] == "--exact-invocations";
var exactReleases = args.Length == 2 && args[1] == "--exact-releases";
var exactSources = args.Length == 2 && args[1] == "--exact-sources";
var image = Path.GetFullPath(args[0]);
var context = new AssemblyLoadContext("Frozen", isCollectible: false);
context.Resolving += (_, name) =>
{
    var path = Path.Combine(image, name.Name + ".dll");
    return File.Exists(path) ? context.LoadFromAssemblyPath(path) : null;
};
var assembly = context.LoadFromAssemblyPath(Path.Combine(image, "Relayer.Benchmarks.dll"));
var handlerType = assembly.GetType("Relayer.Benchmarks.ConcurrentHandler", throwOnError: true)!;
var method = handlerType.GetMethod("HandleAsync")!;
var parameters = method.GetParameters();
var signalsType = assembly.GetType("Relayer.Benchmarks.EndToEndSignals", throwOnError: true)!;
var signals = Activator.CreateInstance(signalsType)!;
var handler = Activator.CreateInstance(handlerType, signals)!;
var run = (Func<Task>)typeof(FixtureControl).GetMethod(nameof(FixtureControl.Measure))!
    .MakeGenericMethod(parameters[0].ParameterType, parameters[1].ParameterType)
    .Invoke(null, [image, handler, method, signals, exactInvocations, exactReleases, exactSources])!;
await run();

static class FixtureControl
{
    public static Func<Task> Measure<TMessage, TContext>(string image, object handler, MethodInfo method, object signals, bool exactInvocations, bool exactReleases, bool exactSources)
    {
        var invoke = method.CreateDelegate<Func<TMessage, TContext, CancellationToken, ValueTask>>(handler);
        var message = (TMessage)Activator.CreateInstance(typeof(TMessage), 42)!;
        var type = signals.GetType();
        var entered = (Channel<byte>)type.GetProperty("Entered")!.GetValue(signals)!;
        var release = (Channel<byte>)type.GetProperty("Release")!.GetValue(signals)!;
        var completed = (Channel<int>)type.GetProperty("Completed")!.GetValue(signals)!;
        // Cache the delegate before warmup. The region includes all eight synchronous writes,
        // including native completion scheduling on this thread; never await inside it.
        Func<int, int, CancellationToken, ValueTask> releaseBatch = (_, _, _) =>
        {
            for (var i = 0; i < 8; i++)
                if (!release.Writer.TryWrite(0)) throw new InvalidOperationException("Release channel is full.");
            return ValueTask.CompletedTask;
        };
        return Run;

        async Task Run()
        {
            using var limit = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            var pending = new ValueTask[8];
            const int warmupBatches = 4096;
            const int measuredBatches = 32768;
            var sources = exactSources ? new SourceAllocations(measuredBatches * 8) : null;
            var invocations = exactInvocations || exactReleases ? new InvocationAllocations(measuredBatches * (exactReleases ? 1 : 8)) : null;
            for (var i = 0; i < warmupBatches; i++) await Batch();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            // Keep phase markers outside the inclusive counter, as in the full-delivery probe.
            if (sources is not null) sources.Enabled = true;
            if (invocations is not null) invocations.Enabled = true;
            FrozenMeasurementEvents.Log.MeasurementStart();
            var allocated = GC.GetTotalAllocatedBytes(precise: true);
            var start = System.Diagnostics.Stopwatch.GetTimestamp();
            long sum = 0;
            for (var i = 0; i < measuredBatches; i++) sum += await Batch();
            var seconds = System.Diagnostics.Stopwatch.GetElapsedTime(start).TotalSeconds;
            var bytes = GC.GetTotalAllocatedBytes(precise: true) - allocated;
            FrozenMeasurementEvents.Log.MeasurementStop();
            if (sources is not null) sources.Enabled = false;
            if (invocations is not null) invocations.Enabled = false;
            if (sum != measuredBatches * 8L * 42) throw new InvalidOperationException($"Unexpected sum: {sum}.");
            var invocationReport = invocations?.Report();
            if (invocationReport is not null && invocationReport.Invocations != measuredBatches * (exactReleases ? 1 : 8))
                throw new InvalidOperationException("Exact invocation count does not match the measured workload.");
            var sourceReport = sources?.Report();
            if (sourceReport is not null && sourceReport.Invocations != measuredBatches * 8)
                throw new InvalidOperationException("Source count does not match measured handler calls.");
            SourceAllocationReport? nativeReads = null;
            if (exactSources)
            {
                // Separate native control after the handler experiment: never subtract these totals.
                var reads = new SourceAllocations(measuredBatches * 8);
                var pendingReads = new ValueTask<byte>[8];
                for (var batch = 0; batch < warmupBatches + measuredBatches; batch++)
                {
                    reads.Enabled = batch >= warmupBatches;
                    for (var i = 0; i < 8; i++) pendingReads[i] = reads.Read(release.Reader, limit.Token);
                    for (var i = 0; i < 8; i++)
                        if (!release.Writer.TryWrite(0)) throw new InvalidOperationException("Native control release failed.");
                    for (var i = 0; i < 8; i++) _ = await pendingReads[i];
                }
                reads.Enabled = false;
                nativeReads = reads.Report();
                if (nativeReads.Invocations != measuredBatches * 8)
                    throw new InvalidOperationException("Source count does not match measured native reads.");
            }
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                sourceAllocations = sourceReport,
                nativeReadSources = nativeReads,
                nativeReadLimits = "Separate post-handler control: eight pending reads on the original release channel with the same cancellable token, then eight writes and completion. Exact synchronous ReadAsync bytes and native source identities only; no registered handler continuations. Different schedule; not a decomposition of handler/end-to-end totals.",
                sourceLimits = "Exact synchronous handler bytes grouped by returned ValueTask source reference identity, observed during warmup and measurement. First observation is not proof of allocation inside this call. Strong-reference tracking, reflection and reporting sit outside byte counters but alter retention/scheduling; inclusive totals include them. No worker continuation or historical attribution, no subtraction from other controls.",
                image, control = "original-handler-and-signal-channels", warmupBatches, measuredBatches,
                instrumentation = exactSources ? "exact-synchronous-source-identities" : exactReleases ? "exact-synchronous-release-batches" : exactInvocations ? "exact-synchronous-invocations" : "none",
                exactRegion = exactReleases ? "Eight synchronous Release.Writer.TryWrite calls per record; includes calling-thread native completion scheduling, excludes other threads and later continuations." : "One synchronous handler delegate call per record.",
                invocationAllocations = invocationReport,
                invocationLimits = exactReleases ? "Exact bytes on the calling thread for the whole synchronous eight-write release batch only. No exact type attribution or historical delta attribution; do not subtract from inclusive totals. Recording changes scheduling; compare within this separate ABBA only." : "Exact bytes on the calling thread during the synchronous handler invocation only. Excludes continuations after suspension, other threads and the diagnostic driver. Includes original handler and called channel/runtime work; no exact type attribution or historical end-to-end delta attribution. Instrumentation changes scheduling; compare only within this separate ABBA.",
                messages = measuredBatches * 8, bytes, bytesPerMessage = bytes / (measuredBatches * 8.0), seconds,
                runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
                arch = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
                limits = "Isolated fixture plus native scheduler and this driver. No DI, JSON, transport or Relayer pipeline. Scheduling differs from full delivery; never subtract this total from end-to-end results or assign the historical delta to it."
            }));

            [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
            async ValueTask<int> Batch()
            {
                for (var i = 0; i < pending.Length; i++)
                {
                    pending[i] = sources is not null
                        ? sources.Invoke(invoke, message, default(TContext)!, limit.Token)
                        : invocations is null || exactReleases
                        ? invoke(message, default!, limit.Token)
                        : invocations.Invoke(invoke, message, default(TContext)!, limit.Token);
                    if (pending[i].IsCompleted) throw new InvalidOperationException("Handler must await release.");
                }
                for (var i = 0; i < pending.Length; i++) await entered.Reader.ReadAsync(limit.Token);
                if (exactReleases)
                    _ = invocations!.Invoke(releaseBatch, 0, 0, limit.Token);
                else
                    for (var i = 0; i < pending.Length; i++)
                        if (!release.Writer.TryWrite(0)) throw new InvalidOperationException("Release channel is full.");
                var total = 0;
                for (var i = 0; i < pending.Length; i++) total += await completed.Reader.ReadAsync(limit.Token);
                // Completion notification can precede handler return. Drain every original ValueTask before reuse.
                for (var i = 0; i < pending.Length; i++) await pending[i];
                return total;
            }
        }
    }
}
