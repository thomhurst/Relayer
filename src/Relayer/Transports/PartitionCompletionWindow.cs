namespace Relayer;

/// <summary>Identifies one delivery in a partition's completion window.</summary>
/// <remarks>
/// Treat tokens as opaque. A default token is invalid. A token is valid only for its original window,
/// until that delivery completes or the window resets. Keeping a token does not retain its window or positions.
/// </remarks>
public readonly struct PartitionCompletionToken
{
    internal PartitionCompletionToken(object owner, int slot, long sequence)
    {
        Owner = owner;
        Slot = slot;
        Sequence = sequence;
    }

    internal object? Owner { get; }
    internal int Slot { get; }
    internal long Sequence { get; }
}

/// <summary>Tracks a bounded, contiguous prefix of completed deliveries for one native partition.</summary>
/// <remarks>
/// One coordinator must serialize all access, including completion reports from concurrent workers.
/// Track every delivery in native arrival order before dispatching it. Complete only after processing
/// and durable failure handling succeed. Positions are opaque: they are never compared or incremented.
/// This window neither commits to a broker nor schedules handlers, pauses fetching or drains work.
/// </remarks>
public sealed class PartitionCompletionWindow<TPosition>
{
    private readonly object _identity = new();
    private readonly Entry[] _entries;
    private int _head;
    private int _tail;
    private int _count;
    private long _sequence;

    /// <summary>Allocates fixed storage for the maximum number of deliveries awaiting contiguous completion.</summary>
    public PartitionCompletionWindow(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _entries = new Entry[capacity];
    }

    /// <summary>The fixed maximum number of retained positions.</summary>
    public int Capacity => _entries.Length;

    /// <summary>The retained delivery count, including completed deliveries waiting behind an unfinished gap.</summary>
    public int Count => _count;

    /// <summary>Whether fetching must stop until the contiguous prefix advances or ownership resets.</summary>
    public bool IsFull => _count == _entries.Length;

    /// <summary>Appends an opaque position in native arrival order and returns its completion token.</summary>
    /// <remarks>
    /// Returns false with a default token when full, without changing the window. The caller must retain
    /// the delivery and apply backpressure; it must never dispatch or skip an untracked delivery.
    /// The caller owns any position storage and must keep it valid until prefix advancement or reset,
    /// and through any subsequent native commit that uses the returned position.
    /// </remarks>
    public bool TryTrack(TPosition position, out PartitionCompletionToken token)
    {
        token = default;
        if (IsFull) return false;
        if (_sequence == long.MaxValue)
            throw new InvalidOperationException("The partition completion token sequence is exhausted. Recreate the window before tracking more deliveries.");

        var sequence = ++_sequence;
        _entries[_tail] = new() { Position = position, Sequence = sequence };
        token = new(_identity, _tail, sequence);
        _tail = Next(_tail);
        _count++;
        return true;
    }

    /// <summary>Accepts a durable completion and releases any newly contiguous completed prefix.</summary>
    /// <param name="token">The token returned for this delivery by <see cref="TryTrack"/>.</param>
    /// <param name="position">The last position of the released prefix, or default when completedCount is zero.</param>
    /// <param name="completedCount">The number of deliveries released, including earlier out-of-order completions.</param>
    /// <returns>
    /// True when this completion was accepted, even if an earlier gap prevents advancement.
    /// False for default, foreign, duplicate or stale tokens, without changing the window.
    /// </returns>
    /// <remarks>
    /// Commit only when completedCount is positive. The returned position is the exact supplied checkpoint;
    /// the transport owns native last-position versus next-position semantics and batching of commits.
    /// This method does not retain a checkpoint for retry if a native commit fails.
    /// </remarks>
    public bool TryComplete(PartitionCompletionToken token, out TPosition? position, out int completedCount)
    {
        position = default;
        completedCount = 0;
        if (!ReferenceEquals(token.Owner, _identity)) return false;
        ref var entry = ref _entries[token.Slot];
        if (entry.Sequence != token.Sequence || entry.Completed) return false;

        entry.Completed = true;
        while (_count > 0 && _entries[_head].Completed)
        {
            position = _entries[_head].Position;
            _entries[_head] = default;
            _head = Next(_head);
            _count--;
            completedCount++;
        }
        return true;
    }

    /// <summary>Discards retained positions and invalidates all outstanding tokens without acknowledging them.</summary>
    /// <remarks>
    /// Use on ownership loss or before reassignment. Stop old dispatch first; the transport separately owns
    /// cancellation, bounded drain, any safe commit before revoke, and replay from the last native commit.
    /// Reset never allows old workers to complete deliveries in a new assignment.
    /// </remarks>
    public void Reset()
    {
        Array.Clear(_entries);
        _head = 0;
        _tail = 0;
        _count = 0;
        // Keep the sequence monotonic across assignments, including when the same slot is reused.
    }

    private int Next(int index) => index + 1 == _entries.Length ? 0 : index + 1;

    private struct Entry
    {
        internal TPosition Position;
        internal long Sequence;
        internal bool Completed;
    }
}
