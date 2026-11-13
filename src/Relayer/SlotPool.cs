using System.Runtime.CompilerServices;

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
                if (Interlocked.Exchange(ref page.Slots[i], null) is { } item) return Unsafe.As<T>(item);
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

    // Fixed sender pools avoid atomic writes to slots already known to be unavailable.
    internal T? TryRentGuarded()
    {
        System.Diagnostics.Debug.Assert(_pageSize == _capacity);
        var slots = _first.Slots;
        for (var i = 0; i < slots.Length; i++)
            if (Volatile.Read(ref slots[i]) is not null &&
                Interlocked.Exchange(ref slots[i], null) is { } item) return Unsafe.As<T>(item);
        return null;
    }

    internal bool TryReturnGuarded(T item)
    {
        System.Diagnostics.Debug.Assert(_pageSize == _capacity);
        var slots = _first.Slots;
        for (var i = 0; i < slots.Length; i++)
            if (Volatile.Read(ref slots[i]) is null &&
                Interlocked.CompareExchange(ref slots[i], item, null) is null) return true;
        return false;
    }

    // Indexed owners use one fixed page so slot identity remains stable through a batch.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal T? TryRent(ref int cursor)
    {
        System.Diagnostics.Debug.Assert(_pageSize == _capacity, "Indexed rentals require one fixed page.");
        var slots = _first.Slots;
        var start = Volatile.Read(ref cursor);
        for (var offset = 0; offset < slots.Length; offset++)
        {
            var index = start + offset;
            if (index >= slots.Length) index -= slots.Length;
            if (Interlocked.Exchange(ref slots[index], null) is not { } item) continue;
            Volatile.Write(ref cursor, index + 1 == slots.Length ? 0 : index + 1);
            return Unsafe.As<T>(item);
        }
        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryReturn(T item, int index)
    {
        System.Diagnostics.Debug.Assert(_pageSize == _capacity, "Indexed returns require one fixed page.");
        return index >= 0 && Interlocked.CompareExchange(ref _first.Slots[index], item, null) is null;
    }

    private sealed class Page(int size)
    {
        // Only typed return methods write these slots. Object storage keeps Interlocked intrinsic
        // in shared generic code; Unsafe.As on rent is safe by that ownership invariant.
        internal readonly object?[] Slots = new object?[size];
        internal Page? Next;
    }
}
