using System.Reflection;
using System.Runtime.InteropServices;

namespace Relayer.Tests;

internal static class TimerQueueWarmup
{
    // CancelAfter reuses its timer after warmup. The timer queue locks and the
    // portable scheduler lock allocate wait handles on first blocking contention.
    // Warm those wait handles before measuring send or batch pipelines.
    internal static void WarmContention()
    {
        const BindingFlags Visibility = BindingFlags.Public | BindingFlags.NonPublic;
        var queuesType = typeof(Timer).Assembly.GetType("System.Threading.TimerQueue");
        var queues = queuesType?.GetProperty("Instances", Visibility | BindingFlags.Static)?.GetValue(null) as Array;
        var sharedLock = queuesType?.GetProperty("SharedLock", Visibility | BindingFlags.Instance);
        var schedulerLock = queuesType?.GetField("s_timerEventLock", Visibility | BindingFlags.Static)?.GetValue(null);

        if (queues is null || sharedLock is null || schedulerLock is not Lock schedulerGate)
        {
            throw UnsupportedRuntime();
        }

        foreach (var queue in queues)
        {
            if (sharedLock.GetValue(queue) is not Lock gate)
            {
                throw UnsupportedRuntime();
            }

            WarmContention(gate);
        }

        WarmContention(schedulerGate);
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
                {
                    failure = new TimeoutException("Timer-queue warmup did not observe the caller waiting for the native Lock.");
                }
            }
        })
        {
            IsBackground = true
        };
        owner.Start();
        held.Wait();
        Volatile.Write(ref entering, 1);

        lock (gate)
        {
        }

        owner.Join();

        if (failure is not null)
        {
            throw failure;
        }
    }

    private static InvalidOperationException UnsupportedRuntime()
    {
        return new(
                $"Cannot find the native TimerQueue.Instances, SharedLock, or s_timerEventLock on {RuntimeInformation.FrameworkDescription}. " +
                "Update the allocation-test warmup for this runtime; keep exact allocation assertions enabled.");
    }
}
