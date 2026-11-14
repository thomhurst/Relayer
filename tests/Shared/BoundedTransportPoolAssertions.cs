using System.Reflection;
using System.Runtime.CompilerServices;

namespace Relayer.Tests.Shared;

internal static class BoundedTransportPoolAssertions
{
    internal static async Task Burst(object pool, Func<object> rent, Action<object> giveBack, int pageSize)
    {
        var references = Fill(rent, giveBack);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        await Assert.That(references.Count(reference => reference.IsAlive)).IsEqualTo(256);
        var first = pool.GetType().GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(field => field.FieldType.Name.StartsWith("SlotPool", StringComparison.Ordinal)).GetValue(pool)!;
        var page = first.GetType().GetField("_first", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(first);
        var pages = 0;
        while (page is not null)
        {
            pages++;
            page = page.GetType().GetField("Next", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(page);
        }
        await Assert.That(pages).IsEqualTo(256 / pageSize);
        var rentals = Enumerable.Range(0, 256).Select(_ => rent()).ToArray();
        await Assert.That(rentals.Distinct(ReferenceEqualityComparer.Instance).Count()).IsEqualTo(256);
        await Assert.That(rentals.All(item => references.Any(reference => ReferenceEquals(reference.Target, item)))).IsTrue();
        foreach (var item in rentals) giveBack(item);
        GC.KeepAlive(pool);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference[] Fill(Func<object> rent, Action<object> giveBack)
    {
        var items = Enumerable.Range(0, 512).Select(_ => rent()).ToArray();
        var references = items.Select(item => new WeakReference(item)).ToArray();
        foreach (var item in items) giveBack(item);
        return references;
    }

    internal static async Task Exclusive(Func<object> rent, Action<object> giveBack)
    {
        var active = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable.Range(0, 32).Select(async _ =>
        {
            await start.Task;
            for (var i = 0; i < 100; i++)
            {
                var item = rent();
                lock (active)
                    if (!active.Add(item)) throw new InvalidOperationException("Concurrent rental of the same state.");
                await Task.Yield();
                lock (active) active.Remove(item);
                giveBack(item);
            }
        }).ToArray();
        start.SetResult();
        await Task.WhenAll(workers);
    }
}
