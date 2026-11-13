using Relayer.TestFixtures;

namespace Relayer.Tests;

public class ConcurrentReceiveAllocationTests
{
    [Test]
    public async Task Pending_cross_thread_receives_allocate_only_native_scopes_after_warmup()
    {
        // Opt-in diagnostic only: retain the real parallel TUnit process, rather than
        // replacing it with an external driver. Load metadata before warmup.
        var region = NativeAllocationRegion.Load(checkpoints: true);
        var workerThreads = region is null ? null : new ulong[2];
        using var fixture = new ConcurrentReceiveFixture(region is null ? null : index => workerThreads![index] = region.CurrentThread());
        for (var i = 0; i < 1000; i++)
        {
            fixture.Run(ConcurrentReceiveFixture.Mode.Scoped);
            fixture.Run(ConcurrentReceiveFixture.Mode.Consumer);
            fixture.Run(ConcurrentReceiveFixture.Mode.Receive);
        }
        if (workerThreads is not null && (workerThreads[0] == 0 || workerThreads[1] == 0 || workerThreads[0] == workerThreads[1]))
            throw new InvalidOperationException("Profiler could not identify both completion workers.");
        // Allocate storage before measuring; formatting/output happens only after every window.
        // These counters describe this execution, not the historical failure or allocation owners.
        var windows = new (Window Scoped, Window Consumer, Window Receive)[1000];
        long scoped = 0, consumer = 0, receive = 0;
        region?.Begin();
        try
        {
            for (var i = 0; i < 1000; i++)
            {
                var native = Capture(fixture, ConcurrentReceiveFixture.Mode.Scoped);
                region?.Checkpoint(i * 3, native.Bytes);
                var consume = Capture(fixture, ConcurrentReceiveFixture.Mode.Consumer);
                region?.Checkpoint(i * 3 + 1, consume.Bytes);
                var fullReceive = Capture(fixture, ConcurrentReceiveFixture.Mode.Receive);
                region?.Checkpoint(i * 3 + 2, fullReceive.Bytes, final: i == windows.Length - 1);
                windows[i] = (native, consume, fullReceive);
                scoped += native.Bytes;
                consumer += consume.Bytes;
                receive += fullReceive.Bytes;
            }
        }
        finally
        {
            try { region?.End(); }
            finally
            {
                // Keep every counter even when the profiler rejects its own report.
                // Formatting/output stays after all measurements and the final marker.
                if (workerThreads is not null)
                    for (var worker = 0; worker < workerThreads.Length; worker++)
                        Console.WriteLine($"allocation-worker={worker}; thread={workerThreads[worker]}");
                Console.WriteLine($"Allocation windows: runtime={System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}; arch={System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}; scoped={scoped}; consumer={consumer}; receive={receive}");
                for (var i = 0; i < windows.Length; i++)
                {
                    var window = windows[i];
                    Console.WriteLine($"window={i}; scoped={window.Scoped}; consumer={window.Consumer}; receive={window.Receive}");
                }
            }
        }
        await Assert.That(scoped).IsGreaterThan(0);
        await Assert.That(consumer).IsEqualTo(scoped);
        await Assert.That(receive).IsEqualTo(scoped);
    }

    private static Window Capture(ConcurrentReceiveFixture fixture, ConcurrentReceiveFixture.Mode mode)
    {
        // GC.CollectionCount does not allocate. Calls bracket Run, outside both thread counters.
        var gen0 = GC.CollectionCount(0);
        var gen1 = GC.CollectionCount(1);
        var gen2 = GC.CollectionCount(2);
        var bytes = fixture.Run(mode);
        return new Window(bytes, fixture.CallerBytes, fixture.WorkerBytes, GC.CollectionCount(0) - gen0,
            GC.CollectionCount(1) - gen1, GC.CollectionCount(2) - gen2);
    }

    private readonly record struct Window(long Bytes, long CallerBytes, long WorkerBytes, int Gen0, int Gen1, int Gen2);
}
