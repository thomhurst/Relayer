using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.MySql.Tests;

public class InboxPoolTests
{
    [Test]
    public async Task Delivery_scopes_reuse_buffers_without_sharing_active_commands()
    {
        var services = new ServiceCollection();
        services.AddRelayerMySqlOutbox();
        await using var provider = services.BuildServiceProvider();
        MySqlInboxPool pool;
        MySqlInboxPool.Scratch first;
        using (var scope = provider.CreateScope())
        {
            pool = scope.ServiceProvider.GetRequiredService<MySqlInboxPools>().Get("orders_");
            first = pool.Rent();
            var concurrent = pool.Rent();
            await Assert.That(ReferenceEquals(first, concurrent)).IsFalse();
            await Assert.That(ReferenceEquals(first.Id, concurrent.Id)).IsFalse();
            pool.Return(first);
            pool.Return(concurrent);
        }
        using (var scope = provider.CreateScope())
        {
            var next = scope.ServiceProvider.GetRequiredService<MySqlInboxPools>().Get("orders_");
            await Assert.That(ReferenceEquals(pool, next)).IsTrue();
            var reused = next.Rent();
            await Assert.That(ReferenceEquals(first, reused)).IsTrue();
            next.Return(reused);
        }
    }

    [Test]
    [Arguments(1)]
    [Arguments(129)]
    public async Task Warm_scratch_rental_allocates_no_managed_bytes(int concurrency)
    {
        using var pool = new MySqlInboxPool(new());
        var items = new MySqlInboxPool.Scratch[concurrency];
        for (var i = 0; i < items.Length; i++) items[i] = pool.Rent();
        foreach (var item in items) pool.Return(item);
        for (var i = 0; i < 1000; i++) pool.Return(pool.Rent());
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < 100; iteration++)
        {
            for (var i = 0; i < items.Length; i++) items[i] = pool.Rent();
            foreach (var item in items) pool.Return(item);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    [Test]
    public async Task Reloaded_prefix_cannot_retarget_an_existing_scope_command()
    {
        using var pools = new MySqlInboxPools();
        var oldPool = pools.Get("old_");
        var old = oldPool.Rent();
        var newPool = pools.Get("new_");
        var current = newPool.Rent();
        await Assert.That(old.Command.CommandText).Contains("`old_inbox`");
        await Assert.That(current.Command.CommandText).Contains("`new_inbox`");
        await Assert.That(ReferenceEquals(pools.Get("old_"), oldPool)).IsTrue();
        oldPool.Return(old);
        newPool.Return(current);
    }
}
