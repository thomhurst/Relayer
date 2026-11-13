namespace Relayer.Tests;

public class SlotPoolTests
{
    [Test]
    [Arguments(32)]
    [Arguments(64)]
    public async Task Retains_every_item_across_multiple_pages(int pageSize)
    {
        var pool = new SlotPool<object>(pageSize);
        var items = Enumerable.Range(0, pageSize * 3 + 1).Select(_ => new object()).ToArray();
        foreach (var item in items) await Assert.That(pool.TryReturn(item)).IsTrue();
        var rented = new HashSet<object>();
        for (var i = 0; i < items.Length; i++) rented.Add(pool.TryRent()!);
        await Assert.That(rented.SetEquals(items)).IsTrue();
        await Assert.That(pool.TryRent()).IsNull();
        foreach (var item in rented) pool.TryReturn(item);
        for (var i = 0; i < items.Length; i++) await Assert.That(pool.TryRent()).IsNotNull();
        await Assert.That(pool.TryRent()).IsNull();
    }

    [Test]
    [Arguments(1)]
    [Arguments(65)]
    [Arguments(256)]
    public async Task Capacity_limits_retention_including_a_partial_last_page(int capacity)
    {
        var pool = new SlotPool<object>(capacity: capacity);
        var retained = new HashSet<object>();
        for (var i = 0; i < capacity; i++)
        {
            var item = new object();
            retained.Add(item);
            await Assert.That(pool.TryReturn(item)).IsTrue();
        }
        var overflow = new object();
        await Assert.That(pool.TryReturn(overflow)).IsFalse();
        var rented = new HashSet<object>();
        for (var i = 0; i < capacity; i++) rented.Add(pool.TryRent()!);
        await Assert.That(rented.SetEquals(retained)).IsTrue();
        await Assert.That(pool.TryRent()).IsNull();
        await Assert.That(pool.TryReturn(overflow)).IsTrue();
        await Assert.That(ReferenceEquals(pool.TryRent(), overflow)).IsTrue();
    }

    [Test]
    public async Task Concurrent_returns_publish_each_item_once_when_pages_grow()
    {
        var pool = new SlotPool<object>(pageSize: 2);
        var items = Enumerable.Range(0, 130).Select(_ => new object()).ToArray();
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var returns = items.Select(async item =>
        {
            await start.Task;
            if (!pool.TryReturn(item)) throw new InvalidOperationException("Unbounded return failed.");
        }).ToArray();
        start.SetResult();
        await Task.WhenAll(returns);
        var rented = new HashSet<object>();
        while (pool.TryRent() is { } item)
            if (!rented.Add(item)) throw new InvalidOperationException("Duplicate reference was rented.");
        await Assert.That(rented.SetEquals(items)).IsTrue();
    }

    [Test]
    public async Task Concurrent_renters_have_exclusive_ownership()
    {
        var pool = new SlotPool<Lease>(pageSize: 2);
        for (var i = 0; i < 16; i++) pool.TryReturn(new());
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable.Range(0, 16).Select(async _ =>
        {
            await start.Task;
            for (var i = 0; i < 1000; i++)
            {
                var item = pool.TryRent() ?? new();
                if (Interlocked.Exchange(ref item.InUse, 1) != 0)
                    throw new InvalidOperationException("Reference rented concurrently.");
                await Task.Yield();
                if (Interlocked.Exchange(ref item.InUse, 0) != 1)
                    throw new InvalidOperationException("Ownership changed while rented.");
                pool.TryReturn(item);
            }
        }).ToArray();
        start.SetResult();
        await Task.WhenAll(workers);
        while (pool.TryRent() is { } item) await Assert.That(item.InUse).IsEqualTo(0);
    }

    private sealed class Lease
    {
        internal int InUse;
    }
}
