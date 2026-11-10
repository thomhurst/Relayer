using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text.Json;
using System.Threading.Channels;

// Diagnostic only. Bind the original handler once; never rebuild historical workload assemblies.
if (args.Length != 1) throw new ArgumentException("Expected the retained benchmark dependency directory.");
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
    .Invoke(null, [image, handler, method, signals])!;
await run();

static class FixtureControl
{
    public static Func<Task> Measure<TMessage, TContext>(string image, object handler, MethodInfo method, object signals)
    {
        var invoke = method.CreateDelegate<Func<TMessage, TContext, CancellationToken, ValueTask>>(handler);
        var message = (TMessage)Activator.CreateInstance(typeof(TMessage), 42)!;
        var type = signals.GetType();
        var entered = (Channel<byte>)type.GetProperty("Entered")!.GetValue(signals)!;
        var release = (Channel<byte>)type.GetProperty("Release")!.GetValue(signals)!;
        var completed = (Channel<int>)type.GetProperty("Completed")!.GetValue(signals)!;
        return Run;

        async Task Run()
        {
            using var limit = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            var pending = new ValueTask[8];
            const int warmupBatches = 4096;
            const int measuredBatches = 32768;
            for (var i = 0; i < warmupBatches; i++) await Batch();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var allocated = GC.GetTotalAllocatedBytes(precise: true);
            var start = System.Diagnostics.Stopwatch.GetTimestamp();
            long sum = 0;
            for (var i = 0; i < measuredBatches; i++) sum += await Batch();
            var seconds = System.Diagnostics.Stopwatch.GetElapsedTime(start).TotalSeconds;
            var bytes = GC.GetTotalAllocatedBytes(precise: true) - allocated;
            if (sum != measuredBatches * 8L * 42) throw new InvalidOperationException($"Unexpected sum: {sum}.");
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                image, control = "original-handler-and-signal-channels", warmupBatches, measuredBatches,
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
                    pending[i] = invoke(message, default!, limit.Token);
                    if (pending[i].IsCompleted) throw new InvalidOperationException("Handler must await release.");
                }
                for (var i = 0; i < pending.Length; i++) await entered.Reader.ReadAsync(limit.Token);
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
