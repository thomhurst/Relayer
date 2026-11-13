using System.Reflection;
using System.Runtime.Loader;

// Bind once to retained test inputs. Never rebuild or copy the fixture into this tool.
if (args.Length != 1) throw new ArgumentException("Expected the retained Relayer.Tests dependency directory.");
var image = Path.GetFullPath(args[0]);
var context = new AssemblyLoadContext("FrozenReceive", isCollectible: false);
context.Resolving += (_, name) =>
{
    var path = Path.Combine(image, name.Name + ".dll");
    return File.Exists(path) ? context.LoadFromAssemblyPath(path) : null;
};
var assembly = context.LoadFromAssemblyPath(Path.Combine(image, "Relayer.Tests.dll"));
var type = assembly.GetType("Relayer.TestFixtures.ConcurrentReceiveFixture", throwOnError: true)!;
using var fixture = (IDisposable)Activator.CreateInstance(type)!;
var mode = type.GetNestedType("Mode")!;
var run = type.GetMethod("Run")!;
var region = NativeAllocationRegion.Load();
typeof(Measurement).GetMethod(nameof(Measurement.Run))!.MakeGenericMethod(mode)
    .Invoke(null, [fixture, run, region]);

internal static class Measurement
{
    public static void Run<TMode>(object fixture, MethodInfo method, NativeAllocationRegion? region)
        where TMode : struct, Enum
    {
        var run = method.CreateDelegate<Func<TMode, long>>(fixture);
        var scoped = Enum.Parse<TMode>("Scoped");
        var consumer = Enum.Parse<TMode>("Consumer");
        var receive = Enum.Parse<TMode>("Receive");
        for (var i = 0; i < 1000; i++)
        {
            _ = run(scoped);
            _ = run(consumer);
            _ = run(receive);
        }
        var windows = new (long Scoped, long Consumer, long Receive)[1000];
        long scopedBytes = 0, consumerBytes = 0, receiveBytes = 0;
        region?.Begin();
        for (var i = 0; i < windows.Length; i++)
        {
            var native = run(scoped);
            var consume = run(consumer);
            var full = run(receive);
            windows[i] = (native, consume, full);
            scopedBytes += native;
            consumerBytes += consume;
            receiveBytes += full;
        }
        region?.End();
        Console.WriteLine($"runtime={System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}; arch={System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}; instrumented={region is not null}; scoped={scopedBytes}; consumer={consumerBytes}; receive={receiveBytes}");
        for (var i = 0; i < windows.Length; i++) Console.WriteLine($"window={i}; scoped={windows[i].Scoped}; consumer={windows[i].Consumer}; receive={windows[i].Receive}");
        if (scopedBytes <= 0 || consumerBytes != scopedBytes || receiveBytes != scopedBytes)
            throw new InvalidOperationException("Exact native scope equality failed; retain this outcome and the complete object report.");
    }
}
