namespace Relayer.Outbox.PostgreSql;

// Like SenderPool, retain overflow on return rather than allocating full segments on rent.
// A rented state stays exclusively owned until the native operation and borrowed rows finish.
internal sealed class PostgreSqlLocalStatePool<T> : IDisposable where T : class
{
    private readonly Func<T> _create;
    private readonly Segment _first;

    internal PostgreSqlLocalStatePool(int capacity, Func<T> create)
    {
        _create = create;
        _first = new(capacity);
        for (var i = 0; i < capacity; i++) _first.Items[i] = create();
    }

    internal T Rent()
    {
        for (var segment = _first; segment is not null; segment = Volatile.Read(ref segment.Next))
            for (var i = 0; i < segment.Items.Length; i++)
                if (Interlocked.Exchange(ref segment.Items[i], null) is { } item) return item;
        return _create();
    }

    internal void Return(T item)
    {
        var segment = _first;
        while (true)
        {
            for (var i = 0; i < segment.Items.Length; i++)
                if (Interlocked.CompareExchange(ref segment.Items[i], item, null) is null) return;
            var next = Volatile.Read(ref segment.Next);
            if (next is null)
                next = Interlocked.CompareExchange(ref segment.Next, new Segment(_first.Items.Length), null) ?? Volatile.Read(ref segment.Next);
            segment = next!;
        }
    }

    // Transport shutdown has already drained in-flight operations.
    public void Dispose()
    {
        for (var segment = _first; segment is not null; segment = segment.Next)
            for (var i = 0; i < segment.Items.Length; i++)
                if (Interlocked.Exchange(ref segment.Items[i], null) is IDisposable disposable) disposable.Dispose();
    }

    private sealed class Segment(int capacity)
    {
        internal readonly T?[] Items = new T[capacity];
        internal Segment? Next;
    }
}
