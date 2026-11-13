namespace Relayer;

/// <summary>Owns idle references; callers own creation, reset and disposal of overflow items.</summary>
internal sealed class SlotPool<T> where T : class
{
    private readonly int _pageSize;
    private readonly int _capacity;
    private readonly Page _first;

    internal SlotPool(int pageSize = 64, int capacity = int.MaxValue)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _pageSize = pageSize;
        _capacity = capacity;
        _first = new(Math.Min(pageSize, capacity));
    }

    internal T? TryRent()
    {
        for (Page? page = _first; page is not null; page = Volatile.Read(ref page.Next))
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.Exchange(ref page.Slots[i], null) is { } item) return item;
        return null;
    }

    internal bool TryReturn(T item)
    {
        var page = _first;
        var remaining = _capacity;
        while (true)
        {
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.CompareExchange(ref page.Slots[i], item, null) is null) return true;
            remaining -= page.Slots.Length;
            if (remaining == 0) return false;
            var next = Volatile.Read(ref page.Next);
            if (next is null)
            {
                var added = new Page(Math.Min(_pageSize, remaining));
                next = Interlocked.CompareExchange(ref page.Next, added, null) ?? added;
            }
            page = next;
        }
    }

    private sealed class Page(int size)
    {
        internal readonly T?[] Slots = new T?[size];
        internal Page? Next;
    }
}
