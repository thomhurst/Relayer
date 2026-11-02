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
        long scoped = 0, consumer = 0, receive = 0;
        for (var i = 0; i < 1000; i++)
        {
            scoped += fixture.Run(ConcurrentReceiveFixture.Mode.Scoped);
            consumer += fixture.Run(ConcurrentReceiveFixture.Mode.Consumer);
            receive += fixture.Run(ConcurrentReceiveFixture.Mode.Receive);
        }
        await Assert.That(scoped).IsGreaterThan(0);
        await Assert.That(consumer).IsEqualTo(scoped);
        await Assert.That(receive).IsEqualTo(scoped);
    }
}
