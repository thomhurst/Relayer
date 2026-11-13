using Relayer.TestFixtures;

namespace Relayer.Tests;

public class ConcurrentReceiveAllocationTests
{
    [Test]
    public async Task Pending_cross_thread_receives_allocate_only_native_scopes_after_warmup()
    {
        using var fixture = new ConcurrentReceiveFixture();
        for (var i = 0; i < 1000; i++)
        {
            fixture.Run(ConcurrentReceiveFixture.Mode.Scoped);
            fixture.Run(ConcurrentReceiveFixture.Mode.Consumer);
            fixture.Run(ConcurrentReceiveFixture.Mode.Receive);
        }
        // Allocate storage before measuring; formatting/output happens only after every window.
        // These counters describe this execution, not the historical failure or allocation owners.
        var windows = new (Window Scoped, Window Consumer, Window Receive)[1000];
        long scoped = 0, consumer = 0, receive = 0;
        for (var i = 0; i < 1000; i++)
        {
            var native = Capture(fixture, ConcurrentReceiveFixture.Mode.Scoped);
            var consume = Capture(fixture, ConcurrentReceiveFixture.Mode.Consumer);
            var fullReceive = Capture(fixture, ConcurrentReceiveFixture.Mode.Receive);
            windows[i] = (native, consume, fullReceive);
            scoped += native.Bytes;
            consumer += consume.Bytes;
            receive += fullReceive.Bytes;
        }
        // Keep all windows, including unfavorable results. No logging runs between measurements.
        Console.WriteLine($"Allocation windows: runtime={System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}; arch={System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture}; scoped={scoped}; consumer={consumer}; receive={receive}");
        for (var i = 0; i < windows.Length; i++)
        {
            var window = windows[i];
            Console.WriteLine($"window={i}; scoped={window.Scoped}; consumer={window.Consumer}; receive={window.Receive}");
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
