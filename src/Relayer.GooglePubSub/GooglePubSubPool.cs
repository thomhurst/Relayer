namespace Relayer.GooglePubSub;

// Match the existing Service Bus pool's retention at the observed concurrency high-water mark.
internal sealed class GooglePubSubPool<T>(Func<T> create) where T : class
{
    private readonly Page _first = new();
    internal T Rent()
    {
        for (Page? page = _first; page is not null; page = Volatile.Read(ref page.Next))
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.Exchange(ref page.Slots[i], null) is { } item) return item;
        return create();
    }
    internal void Return(T item)
    {
        var page = _first;
        while (true)
        {
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.CompareExchange(ref page.Slots[i], item, null) is null) return;
            var next = Volatile.Read(ref page.Next);
            if (next is null) next = Interlocked.CompareExchange(ref page.Next, new Page(), null) ?? page.Next!;
            page = next;
        }
    }
    private sealed class Page
    {
        internal readonly T?[] Slots = new T[64];
        internal Page? Next;
    }
}
