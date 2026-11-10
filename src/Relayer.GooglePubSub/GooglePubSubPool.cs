namespace Relayer.GooglePubSub;

// Bound retained native-boundary state (D52); overflow is collected and misses allocate.
internal sealed class GooglePubSubPool<T>(Func<T> create) where T : class
{
    internal const int Capacity = 256;
    private const int PageSize = 64;
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
        for (var pageIndex = 0; pageIndex < Capacity / PageSize; pageIndex++)
        {
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.CompareExchange(ref page.Slots[i], item, null) is null) return;
            if (pageIndex == Capacity / PageSize - 1) return;
            var next = Volatile.Read(ref page.Next);
            if (next is null) next = Interlocked.CompareExchange(ref page.Next, new Page(), null) ?? page.Next!;
            page = next;
        }
    }
    private sealed class Page
    {
        internal readonly T?[] Slots = new T[PageSize];
        internal Page? Next;
    }
}
