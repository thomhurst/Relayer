using System.Reflection;
using System.Runtime.InteropServices;

namespace Relayer.Tests;

internal static class TimerQueueWarmup
{
    // CancelAfter reuses its timer after warmup, but .NET's shared TimerQueue Lock
    // allocates a 24 B AutoResetEvent and 32 B SafeWaitHandle on first contention.
    // Exercise that native lazy initialization before measuring send or batch pipelines.
    // This is setup only: do not replace native timers or exclude bytes from a window.
    internal static void WarmContention()
    {
        const BindingFlags visibility = BindingFlags.Public | BindingFlags.NonPublic;
        var queuesType = typeof(Timer).Assembly.GetType("System.Threading.TimerQueue");
        var queues = queuesType?.GetProperty("Instances", visibility | BindingFlags.Static)?.GetValue(null) as Array;
        var sharedLock = queuesType?.GetProperty("SharedLock", visibility | BindingFlags.Instance);
        if (queues is null || sharedLock is null)
            throw UnsupportedRuntime();

        foreach (var queue in queues)
        {
            if (sharedLock.GetValue(queue) is not Lock gate) throw UnsupportedRuntime();
            WarmContention(gate);
        }
    }

    private static void WarmContention(Lock gate)
    {
        var caller = Thread.CurrentThread;
        using var held = new ManualResetEventSlim();
        var entering = 0;
        Exception? failure = null;
        var owner = new Thread(() =>
        {
            lock (gate)
            {
                held.Set();
                if (!SpinWait.SpinUntil(() => Volatile.Read(ref entering) != 0 &&
                        (caller.ThreadState & ThreadState.WaitSleepJoin) != 0, TimeSpan.FromSeconds(10)))
                    failure = new TimeoutException("Timer-queue warmup did not observe the caller waiting for the native Lock.");
            }
        }) { IsBackground = true };
        owner.Start();
        held.Wait();
        Volatile.Write(ref entering, 1);
        lock (gate) { }
        owner.Join();
        if (failure is not null) throw failure;
    }

    private static InvalidOperationException UnsupportedRuntime() => new(
        $"Cannot find the native TimerQueue.Instances/SharedLock on {RuntimeInformation.FrameworkDescription}. " +
        "Update the allocation-test warmup for this runtime; keep exact allocation assertions enabled.");
}
