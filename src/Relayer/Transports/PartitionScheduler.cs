using Relayer.Pipeline;
namespace Relayer.Transports;

/// <summary>Schedules native partition deliveries through an existing receive/failure pipeline.</summary>
/// <remarks>
/// Serialize every call on one transport-owned pump, including rebalance callbacks. Pump regularly even
/// when fetching is paused: handler continuations never call native operations. Pass the composed
/// ReceivePipeline.ProcessAsync delegate; only Complete and SettledByHandler are durable successes.
/// Keep pumping after a drain deadline or loss until BufferedCount reaches zero, so late handlers are
/// observed and their borrowed storage released. Never dispose native buffers while handlers use them.
/// </remarks>
public sealed class PartitionScheduler<TIncoming, TBinding, TPartition, TPosition>
    where TBinding : struct, IPartitionMessageBinding<TIncoming>
{
    private readonly object _identity = new();
    private readonly IPartitionSource<TPartition, TPosition> _source;
    private readonly Func<TIncoming, CancellationToken, ValueTask<Disposition>> _receive;
    private readonly PartitionSchedulerOptions _options;
    private readonly TimeProvider _clock;
    private readonly List<Assignment> _assignments = [];
    private int _dispatchCursor;
    private bool _stopping;

    /// <summary>Creates a scheduler; cache the receive delegate once, sized for the configured concurrency.</summary>
    public PartitionScheduler(IPartitionSource<TPartition, TPosition> source,
        Func<TIncoming, CancellationToken, ValueTask<Disposition>> receive,
        PartitionSchedulerOptions? options = null, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(receive);
        _options = options ?? new();
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.MaxBufferedPerPartition, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.MaxBufferedDeliveries, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.MaxConcurrency, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.ConcurrencyPerPartition, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(_options.MaxBufferedPerPartition, _options.MaxBufferedDeliveries);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(_options.MaxConcurrency, _options.MaxBufferedDeliveries);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(_options.ConcurrencyPerPartition, _options.MaxBufferedPerPartition);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(_options.ConcurrencyPerPartition, _options.MaxConcurrency);
        _source = source;
        _receive = receive;
        _clock = timeProvider ?? TimeProvider.System;
        LastProgressAt = _clock.GetUtcNow();
    }

    /// <summary>Retained deliveries, including late active work after ownership ends.</summary>
    public int BufferedCount { get; private set; }
    /// <summary>Handlers still running, including those that ignore cancellation after ownership ends.</summary>
    public int ActiveCount { get; private set; }
    /// <summary>Whether all requested revocations have ended. Late handlers can still retain storage.</summary>
    public bool IsDraining
    {
        get
        {
            for (var i = 0; i < _assignments.Count; i++)
                if (_assignments[i].Revoking && !_assignments[i].Fenced) return true;
            return false;
        }
    }
    /// <summary>Last observed durable pipeline completion, for the transport's consumer liveness provider.</summary>
    /// <remarks>Use BufferedCount == 0 for idle. A failed gap is not idle even when ActiveCount is zero.</remarks>
    public DateTimeOffset LastProgressAt { get; private set; }

    /// <summary>Creates a fresh epoch. Do not reassign retained partitions during cooperative rebalancing.</summary>
    public PartitionAssignmentToken Assign(TPartition partition)
    {
        if (_stopping) throw new InvalidOperationException("The partition scheduler is stopping.");
        for (var i = 0; i < _assignments.Count; i++)
            if (!_assignments[i].Fenced && EqualityComparer<TPartition>.Default.Equals(_assignments[i].Partition, partition))
                throw new InvalidOperationException("The partition is already assigned. Retain its existing assignment token.");
        var assignment = new Assignment(_identity, partition, _options.MaxBufferedPerPartition);
        try { UpdatePause(assignment); }
        catch { assignment.Cancellation.Dispose(); throw; }
        _assignments.Add(assignment);
        return new(assignment);
    }

    /// <summary>Accepts a delivery in native arrival order. False leaves ownership with the caller.</summary>
    /// <remarks>On false, retain and retry the same delivery before admitting later records from this partition.</remarks>
    public bool TryEnqueue(PartitionAssignmentToken token, TIncoming message, TPosition position)
    {
        var assignment = Get(token);
        if (_stopping || assignment is null || assignment.Fenced || assignment.Revoking || assignment.Failure is not null) return false;
        if (assignment.Count == assignment.Entries.Length || BufferedCount == _options.MaxBufferedDeliveries)
            return false;
        var keyHead = FindKeyHead(assignment, message);
        if (!assignment.Window.TryTrack(position, out var completion))
            throw new InvalidOperationException("The partition completion window and delivery storage diverged.");
        var tail = (assignment.Head + assignment.Count) % assignment.Entries.Length;
        assignment.Entries[tail] = new()
        {
            Message = message, Token = completion, State = EntryState.Queued,
            NextKey = -1, NextReady = -1, KeyTail = tail, KeyHead = keyHead < 0,
        };
        if (keyHead < 0) MakeReady(assignment, tail);
        else
        {
            ref var head = ref assignment.Entries[keyHead];
            assignment.Entries[head.KeyTail].NextKey = tail;
            head.KeyTail = tail;
        }
        assignment.Count++;
        BufferedCount++;
        return true;
    }

    /// <summary>Returns the pipeline failure that stopped this assignment, or null.</summary>
    /// <remarks>Report through the transport's existing worker health/error path. Recover by revoking and replaying, never by skipping the gap.</remarks>
    public Exception? GetFailure(PartitionAssignmentToken token) => Get(token)?.Failure;

    /// <summary>Stops dispatch immediately and drains active work until the monotonic deadline.</summary>
    /// <remarks>Queued work is replayed. Call Pump until IsDraining is false before returning native revocation.</remarks>
    public void Revoke(PartitionAssignmentToken token, TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        var assignment = Get(token);
        if (assignment is null || assignment.Fenced || assignment.Revoking) return;
        assignment.Revoking = true;
        assignment.DrainStarted = _clock.GetTimestamp();
        assignment.DrainTimeout = timeout;
        UpdatePause(assignment);
    }

    /// <summary>Fences ownership immediately, without publishing any further checkpoints or resuming fetching.</summary>
    public void Lose(PartitionAssignmentToken token)
    {
        var assignment = Get(token);
        if (assignment is not null && !assignment.Fenced) Fence(assignment);
    }

    /// <summary>Rejects new assignments and starts bounded draining for every owned partition.</summary>
    /// <remarks>Shortens existing revocation deadlines without extending them. Every assignment starts draining before native pause callbacks run.</remarks>
    public void Stop(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        _stopping = true;
        var started = _clock.GetTimestamp();
        for (var i = 0; i < _assignments.Count; i++)
        {
            var assignment = _assignments[i];
            if (assignment.Fenced) continue;
            if (!assignment.Revoking ||
                timeout < assignment.DrainTimeout - _clock.GetElapsedTime(assignment.DrainStarted, started))
            {
                assignment.Revoking = true;
                assignment.DrainStarted = started;
                assignment.DrainTimeout = timeout;
            }
        }
        // Native pause can throw. Apply shutdown to every assignment before calling the source.
        for (var i = 0; i < _assignments.Count; i++) UpdatePause(_assignments[i]);
    }

    /// <summary>Observes completions, publishes safe checkpoints, dispatches eligible keys, and updates native backpressure.</summary>
    /// <remarks>No waits, worker tasks, locks or allocations. Native callback errors propagate; call again to retry pending checkpoint capture.</remarks>
    public void Pump()
    {
        // A native checkpoint callback can throw. Fence every expired assignment before invoking any.
        for (var i = 0; i < _assignments.Count; i++)
        {
            var assignment = _assignments[i];
            if (!assignment.Fenced && assignment.Revoking &&
                _clock.GetElapsedTime(assignment.DrainStarted) >= assignment.DrainTimeout)
                Fence(assignment);
        }
        // Observe every partition before dispatch, so retained partitions can use newly freed global capacity.
        for (var i = 0; i < _assignments.Count; i++)
        {
            var assignment = _assignments[i];
            Publish(assignment);
            var head = assignment.Head;
            var count = assignment.Count;
            // Fenced entries can be scattered, because queued entries were discarded on loss.
            var slots = assignment.Fenced ? assignment.Entries.Length : count;
            for (var offset = 0; offset < slots && assignment.Active != 0; offset++)
            {
                var slot = assignment.Fenced ? offset : (head + offset) % assignment.Entries.Length;
                if (assignment.Entries[slot].State == EntryState.Active && assignment.Entries[slot].Pending.IsCompleted)
                    Complete(assignment, slot);
            }
            if (assignment.Revoking && !assignment.Fenced && assignment.Active == 0) Fence(assignment);
        }
        var idle = 0;
        while (ActiveCount < _options.MaxConcurrency && idle < _assignments.Count)
        {
            if (_dispatchCursor >= _assignments.Count) _dispatchCursor = 0;
            var assignment = _assignments[_dispatchCursor++];
            if (assignment.Fenced || assignment.Revoking || assignment.Failure is not null ||
                assignment.Active == _options.ConcurrencyPerPartition || assignment.ReadyHead < 0)
            {
                idle++;
                continue;
            }
            Dispatch(assignment);
            idle = 0;
        }
        for (var i = 0; i < _assignments.Count; i++) UpdatePause(_assignments[i]);
        for (var i = _assignments.Count - 1; i >= 0; i--)
        {
            var assignment = _assignments[i];
            if (!assignment.Fenced || assignment.Active != 0) continue;
            assignment.Cancellation.Dispose();
            _assignments.RemoveAt(i);
            if (i < _dispatchCursor) _dispatchCursor--;
        }
    }

    private Assignment? Get(PartitionAssignmentToken token) =>
        token.State is Assignment assignment && ReferenceEquals(assignment.Owner, _identity) ? assignment : null;

    private void Dispatch(Assignment assignment)
    {
        var slot = assignment.ReadyHead;
        ref var entry = ref assignment.Entries[slot];
        assignment.ReadyHead = entry.NextReady;
        if (assignment.ReadyHead < 0) assignment.ReadyTail = -1;
        entry.NextReady = -1;
        entry.State = EntryState.Active;
        assignment.Active++;
        ActiveCount++;
        try { entry.Pending = _receive(entry.Message, assignment.Cancellation.Token); }
        catch (Exception error)
        {
            Fail(assignment, ref entry, error);
            return;
        }
        if (entry.Pending.IsCompleted) Complete(assignment, slot);
    }

    private static int FindKeyHead(Assignment assignment, TIncoming message)
    {
        for (var offset = 0; offset < assignment.Count; offset++)
        {
            var slot = (assignment.Head + offset) % assignment.Entries.Length;
            ref var entry = ref assignment.Entries[slot];
            if (entry.KeyHead && TBinding.KeysEqual(entry.Message, message)) return slot;
        }
        return -1;
    }

    private static void MakeReady(Assignment assignment, int slot)
    {
        if (assignment.ReadyTail < 0) assignment.ReadyHead = slot;
        else assignment.Entries[assignment.ReadyTail].NextReady = slot;
        assignment.ReadyTail = slot;
    }

    private static void PromoteKey(Assignment assignment, ref Entry entry)
    {
        entry.KeyHead = false;
        if (entry.NextKey < 0) return;
        ref var next = ref assignment.Entries[entry.NextKey];
        next.KeyHead = true;
        next.KeyTail = entry.KeyTail;
        MakeReady(assignment, entry.NextKey);
    }

    private void Complete(Assignment assignment, int slot)
    {
        ref var entry = ref assignment.Entries[slot];
        Disposition disposition;
        try
        {
            // IsCompleted was checked by the pump. Consume each ValueTask exactly once, without blocking.
            disposition = entry.Pending.GetAwaiter().GetResult();
            if (!assignment.Fenced && disposition.Kind is not (DispositionKind.Complete or DispositionKind.SettledByHandler))
                throw new InvalidOperationException("Partition receive must finish durable failure handling before returning Complete. Native redelivery and dead-letter dispositions cannot advance a log checkpoint.");
        }
        catch (Exception error)
        {
            Fail(assignment, ref entry, error);
            if (assignment.Fenced) Release(assignment, slot);
            return;
        }
        entry.Pending = default;
        entry.State = EntryState.Completed;
        assignment.Active--;
        ActiveCount--;
        if (assignment.Fenced) { Release(assignment, slot); return; }
        PromoteKey(assignment, ref entry);
        LastProgressAt = _clock.GetUtcNow();
        assignment.Window.TryComplete(entry.Token, out var position, out var count);
        if (count == 0) return;
        assignment.Checkpoint = position!;
        assignment.PublishCount = count;
        Publish(assignment);
    }

    private void Fail(Assignment assignment, ref Entry entry, Exception error)
    {
        entry.Pending = default;
        entry.State = EntryState.Failed;
        assignment.Active--;
        ActiveCount--;
        assignment.Failure ??= error;
    }

    private void Publish(Assignment assignment)
    {
        if (assignment.Fenced || assignment.PublishCount == 0) return;
        _source.Commit(assignment.Partition, assignment.Checkpoint);
        var count = assignment.PublishCount;
        assignment.PublishCount = 0;
        assignment.Checkpoint = default!;
        for (var i = 0; i < count; i++)
        {
            Release(assignment, assignment.Head);
            assignment.Head = (assignment.Head + 1) % assignment.Entries.Length;
        }
    }

    private void Fence(Assignment assignment)
    {
        assignment.Fenced = true;
        assignment.Window.Reset();
        assignment.PublishCount = 0;
        assignment.Checkpoint = default!;
        // Fence before cancellation: cancellation callbacks may finish handlers synchronously.
        try { assignment.Cancellation.Cancel(); }
        finally
        {
            for (var slot = 0; slot < assignment.Entries.Length; slot++)
                if (assignment.Entries[slot].State is not (EntryState.Empty or EntryState.Active)) Release(assignment, slot);
        }
    }

    private void Release(Assignment assignment, int slot)
    {
        TBinding.Release(assignment.Entries[slot].Message);
        assignment.Entries[slot] = default;
        assignment.Count--;
        BufferedCount--;
    }

    private void UpdatePause(Assignment assignment)
    {
        if (assignment.Fenced) return;
        var pause = assignment.Revoking || assignment.Failure is not null ||
            assignment.Count == assignment.Entries.Length || BufferedCount == _options.MaxBufferedDeliveries;
        if (assignment.PauseKnown && pause == assignment.Paused) return;
        if (pause) _source.Pause(assignment.Partition);
        else _source.Resume(assignment.Partition);
        assignment.Paused = pause;
        assignment.PauseKnown = true;
    }

    private enum EntryState : byte { Empty, Queued, Active, Completed, Failed }

    private struct Entry
    {
        internal TIncoming Message;
        internal PartitionCompletionToken Token;
        internal ValueTask<Disposition> Pending;
        internal EntryState State;
        internal bool KeyHead;
        internal int KeyTail;
        internal int NextKey;
        internal int NextReady;
    }

    private sealed class Assignment(object owner, TPartition partition, int capacity)
    {
        internal readonly object Owner = owner;
        internal readonly TPartition Partition = partition;
        internal readonly Entry[] Entries = new Entry[capacity];
        internal readonly PartitionCompletionWindow<TPosition> Window = new(capacity);
        internal readonly CancellationTokenSource Cancellation = new();
        internal int Head;
        internal int Count;
        internal int Active;
        internal int ReadyHead = -1;
        internal int ReadyTail = -1;
        internal bool Paused;
        internal bool PauseKnown;
        internal bool Revoking;
        internal bool Fenced;
        internal long DrainStarted;
        internal TimeSpan DrainTimeout;
        internal Exception? Failure;
        internal TPosition Checkpoint = default!;
        internal int PublishCount;
    }
}
