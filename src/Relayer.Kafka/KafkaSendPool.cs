using Dekaf.Serialization;

namespace Relayer.Kafka;

// Retain every operation at the observed concurrency high-water mark. A fixed retention
// cap would allocate on every send once steady-state concurrency exceeds that cap.
internal sealed class KafkaSendPool
{
    private readonly Page _first = new();

    internal KafkaSendOperation Rent(int count)
    {
        for (Page? page = _first; page is not null; page = Volatile.Read(ref page.Next))
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.Exchange(ref page.Slots[i], null) is { } operation)
                {
                    operation.Ensure(count);
                    return operation;
                }
        var created = new KafkaSendOperation();
        created.Ensure(count);
        return created;
    }

    internal void Return(KafkaSendOperation operation)
    {
        operation.Clear();
        var page = _first;
        while (true)
        {
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.CompareExchange(ref page.Slots[i], operation, null) is null) return;
            var next = Volatile.Read(ref page.Next);
            if (next is null)
            {
                var created = new Page();
                next = Interlocked.CompareExchange(ref page.Next, created, null) ?? created;
            }
            page = next;
        }
    }

    private sealed class Page
    {
        internal readonly KafkaSendOperation?[] Slots = new KafkaSendOperation?[64];
        internal Page? Next;
    }
}

internal sealed class KafkaSendOperation
{
    internal KafkaSendState[] States = [];
    internal readonly List<(WireString? Key, (ReadOnlyMemory<byte> Body, Headers Headers) Value)> Messages = [];
    private int _count;

    internal void Ensure(int count)
    {
        if (States.Length < count)
        {
            var previous = States.Length;
            Array.Resize(ref States, count);
            for (var i = previous; i < count; i++) States[i] = new();
        }
        Messages.EnsureCapacity(count);
        _count = count;
    }

    internal void Clear()
    {
        for (var i = 0; i < _count; i++) States[i].Clear();
        Messages.Clear();
        _count = 0;
    }
}
