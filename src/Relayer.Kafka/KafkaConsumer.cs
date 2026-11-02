using System.Diagnostics;
using System.Runtime.CompilerServices;
using Dekaf;
using Dekaf.Consumer;

namespace Relayer.Kafka;

// One owner pumps the core scheduler and native operations. Handler continuations only signal it.
internal sealed class KafkaConsumer : IAsyncDisposable
{
    private readonly IKafkaConsumer<byte[], byte[]> _native;
    private readonly MessageOperationTracing _commit;
    private readonly KafkaOwnership _ownership;
    private readonly KafkaWorkerHealth? _health;
    private readonly KafkaConsumerOptions _options;
    private readonly KafkaPartitionSource _source;
    private PartitionScheduler<KafkaDelivery, KafkaMessageBinding, TopicPartition, KafkaCheckpoint> _scheduler;
    private readonly Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> _receive;
    private readonly int _concurrency;
    private readonly KafkaPumpSignal _wake = new();
    private readonly Action _signal;
    private readonly List<KafkaRebalance> _revokes = [];
    private readonly KafkaReplay _replay = new();
    private CancellationTokenSource _stop = new();
    private CancellationTokenSource _abort = new();
    private TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _stopping;
    private bool _restarting;
    private bool _disposed;
    private readonly Timer _deadline;
    internal string Topic { get; }
    internal string? Group { get; }
    internal Task Completion { get; private set; } = Task.CompletedTask;

    internal KafkaConsumer(IKafkaConsumer<byte[], byte[]> native,
        Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive, MessageTracing tracing, MessagingEndpoint endpoint,
        KafkaOwnership ownership, KafkaWorkerHealth? health = null, KafkaConsumerOptions? options = null, int concurrency = 1)
    {
        _native = native;
        _ownership = ownership;
        _health = health;
        _options = options ?? new();
        _receive = receive;
        _concurrency = concurrency;
        _source = new(native);
        _signal = _wake.Signal;
        _deadline = new(static state => ((KafkaPumpSignal)state!).Signal(), _wake, Timeout.Infinite, Timeout.Infinite);
        _ownership.Wake = _signal;
        _scheduler = new(_source, receive, _options.SchedulerOptions(concurrency), _health?.Clock, _signal);
        Topic = endpoint.Destination;
        Group = endpoint.ConsumerGroup;
        _commit = new(tracing, endpoint, MessagingOperation.Settle, "commit");
    }

    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!Completion.IsCompleted) throw new InvalidOperationException("The Kafka consumer is already running.");
        if (Completion.IsFaulted || Completion.IsCanceled || _abort.IsCancellationRequested || _scheduler.BufferedCount != 0)
            throw new InvalidOperationException("Recreate the application host after a failed or aborted Kafka receive. A new native consumer resumes at the broker's committed offset.");
        if (_stop.IsCancellationRequested)
        {
            _stop.Dispose();
            _abort.Dispose();
            _stop = new();
            _abort = new();
            _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _stopping = false;
            _restarting = true;
            _ownership.Restart();
        }
        _replay.Start();
        if (ExecutionContext.IsFlowSuppressed()) Completion = Task.Run(RunAsync);
        else { using (ExecutionContext.SuppressFlow()) Completion = Task.Run(RunAsync); }
        await _started.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task RunAsync()
    {
        using var stopping = _stop.Token.UnsafeRegister(static state => ((KafkaPumpSignal)state!).Signal(), _wake);
        using var aborting = _abort.Token.UnsafeRegister(static state => ((KafkaPumpSignal)state!).Signal(), _wake);
        try
        {
            await ObserveAsync(_native.InitializeAsync(_stop.Token)).ConfigureAwait(false);
            if (_restarting) await ResetAssignmentsAsync().ConfigureAwait(false);
            _health?.Idle();
            _started.TrySetResult();
            while (!_stop.IsCancellationRequested)
            {
                using var poll = _replay.BeginPolling(_stop.Token);
                try
                {
                    await using var batches = _native.ConsumeBatchAsync(poll.Token).GetAsyncEnumerator();
                    ValueTask<bool> next = default;
                    var pending = false;
                    try
                    {
                        while (!poll.IsCancellationRequested)
                        {
                            next = batches.MoveNextAsync();
                            pending = true;
                            if (!next.IsCompleted) next.GetAwaiter().UnsafeOnCompleted(_signal);
                            while (!next.IsCompleted)
                            {
                                await PumpAsync().ConfigureAwait(false);
                                if (_stop.IsCancellationRequested) BeginStop();
                                await _wake.WaitAsync().ConfigureAwait(false);
                            }
                            bool available;
                            pending = false;
                            try { available = await next.ConfigureAwait(false); }
                            catch (OperationCanceledException) when (poll.IsCancellationRequested) { break; }
                            if (!available) break;
                            await AdmitBatchAsync(batches.Current).ConfigureAwait(false);
                        }
                        // A replay waits for admitted work and its commits before changing positions.
                        if (!_stop.IsCancellationRequested) await DrainAcceptedAsync().ConfigureAwait(false);
                    }
                    catch (Exception error)
                    {
                        // An iterator cannot be disposed while MoveNextAsync is outstanding.
                        // Cancel polling and finish that operation without masking the pipeline error.
                        poll.Cancel();
                        _ownership.Stop(error);
                        foreach (var revoke in _revokes) revoke.Completion.TrySetException(error);
                        if (pending)
                        {
                            while (!next.IsCompleted) await _wake.WaitAsync().ConfigureAwait(false);
                            try { await next.ConfigureAwait(false); }
                            catch (Exception) { /* Preserve the original receive failure. */ }
                        }
                        throw;
                    }
                }
                finally { _replay.EndPolling(); }
                while (_replay.TryRead(out var request))
                {
                    try { await ApplySeekAsync(request).ConfigureAwait(false); request.Completion.TrySetResult(); }
                    catch (OperationCanceledException) { request.Completion.TrySetCanceled(request.Token); }
                    catch (Exception error) { request.Completion.TrySetException(error); }
                }
                if (!poll.IsCancellationRequested) break;
            }
            BeginStop();
            while (_scheduler.IsDraining)
            {
                await PumpAsync().ConfigureAwait(false);
                if (_scheduler.IsDraining) await _wake.WaitAsync().ConfigureAwait(false);
            }
            await FlushAsync(null).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (Exception error) { _started.TrySetException(error); throw; }
        finally
        {
            _started.TrySetCanceled();
            _deadline.Change(Timeout.Infinite, Timeout.Infinite);
            _scheduler.Stop(TimeSpan.Zero);
            // Snapshots keep ignored-cancellation handlers safe after a bounded stop.
            // DisposeAsync observes and releases those late operations before native disposal.
            _ownership.Stop(new InvalidOperationException("Kafka consumer stopped."));
            foreach (var revoke in _revokes) revoke.Completion.TrySetException(new InvalidOperationException("Kafka consumer stopped."));
            _revokes.Clear();
            _replay.Stop();
            _health?.Unavailable();
        }
    }

    private async ValueTask ResetAssignmentsAsync()
    {
        // A bounded stop discards queued records. The native fetch position is therefore
        // not a durability boundary: restart from broker commits, or the first admitted offset.
        var partitions = _native.Assignment.ToArray();
        var committed = await _native.GetCommittedOffsetsAsync(partitions, _stop.Token).ConfigureAwait(false);
        foreach (var partition in partitions)
        {
            if (committed.TryGetValue(partition, out var offset) && offset.Offset >= 0) _native.Seek(offset);
            else if (_source.Assignments.TryGetValue(partition, out var old) && old.FirstOffset is { } first) _native.Seek(first);
        }
        _source.Assignments.Clear();
        _scheduler = new(_source, _receive, _options.SchedulerOptions(_concurrency), _health?.Clock, _signal);
        foreach (var partition in partitions) Assign(partition);
        _restarting = false;
    }

    private async ValueTask ObserveAsync(ValueTask operation)
    {
        if (!operation.IsCompleted) operation.GetAwaiter().UnsafeOnCompleted(_signal);
        while (!operation.IsCompleted)
        {
            await PumpAsync().ConfigureAwait(false);
            await _wake.WaitAsync().ConfigureAwait(false);
        }
        await operation.ConfigureAwait(false);
    }

    private void BeginStop()
    {
        if (_stopping) return;
        _stopping = true;
        _scheduler.Stop(_abort.IsCancellationRequested ? TimeSpan.Zero : _options.DrainTimeout);
        _deadline.Change(TimeSpan.Zero, TimeSpan.FromMilliseconds(5));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AdmitBatchAsync(ConsumeBatch<byte[], byte[]> batch)
    {
        var partition = batch.TopicPartition;
        if (!_source.Assignments.TryGetValue(partition, out var assignment))
        {
            if (_native.Subscription.Count != 0 || !_native.Assignment.Contains(partition)) return;
            assignment = Assign(partition); // Supports explicit native Assign as well as group subscriptions.
        }
        var records = batch.GetEnumerator();
        while (!_stop.IsCancellationRequested && !assignment.Revoking && _source.Assignments.TryGetValue(partition, out var current) && ReferenceEquals(assignment, current))
        {
            if (_scheduler.BufferedCount == _options.MaxBufferedDeliveries ||
                assignment.Delivered - assignment.Durable == _options.MaxBufferedPerPartition)
                break; // Native Pause holds the undisclosed remainder while other partitions continue.
            if (!records.MoveNext()) break;
            if (!batch.TryGetNextOffset(out var checkpoint)) break;
            var delivery = KafkaDelivery.Snapshot(records.Current, assignment.PartitionId);
            assignment.FirstOffset ??= records.Current.TopicPartitionOffset;
            var sequence = assignment.Delivered + 1;
            if (!_scheduler.TryEnqueue(assignment.Token, delivery, new(checkpoint, sequence)))
                throw new InvalidOperationException("Kafka scheduler rejected a delivery with available capacity.");
            assignment.Delivered = sequence;
            _scheduler.Pump();
            if (_ownership.HasCommands && _ownership.TryRead(out var command)) HandleCommand(command!);
            ThrowFailures();
        }
        if (!assignment.Revoking && batch.TryGetNextOffset(out var tail)) _source.CaptureTail(assignment, tail);
        await PumpAsync().ConfigureAwait(false);
    }

    private KafkaPartitionSource.Assignment Assign(TopicPartition partition)
    {
        var assignment = new KafkaPartitionSource.Assignment(partition);
        _source.Assignments.Add(partition, assignment);
        assignment.Token = _scheduler.Assign(partition);
        return assignment;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PumpAsync()
    {
        while (_ownership.TryRead(out var command)) HandleCommand(command!);
        if (_abort.IsCancellationRequested) _scheduler.Stop(TimeSpan.Zero);
        _scheduler.Pump();
        for (var i = _revokes.Count - 1; i >= 0; i--)
        {
            var revoke = _revokes[i];
            var draining = false;
            foreach (var partition in revoke.Partitions)
                if (_source.Assignments.TryGetValue(partition, out var assignment) && _scheduler.IsAssigned(assignment.Token)) draining = true;
            if (draining) continue;
            try { await FlushAsync(revoke).ConfigureAwait(false); revoke.Completion.TrySetResult(); }
            catch (Exception error) { revoke.Completion.TrySetException(error); throw; }
            foreach (var partition in revoke.Partitions) _source.Assignments.Remove(partition);
            _revokes.RemoveAt(i);
            if (_revokes.Count == 0 && !_stopping) _deadline.Change(Timeout.Infinite, Timeout.Infinite);
        }
        ThrowFailures();
        await FlushAsync(null).ConfigureAwait(false);
        if (_health is not null)
        {
            if (_scheduler.OldestOutstandingAt is { } oldest) _health.ActiveAt(oldest);
            else _health.Idle();
        }
    }

    private void HandleCommand(KafkaRebalance command)
    {
        command.Owner = this;
        if (command.Context is { } context)
            ExecutionContext.Run(context, static state =>
            {
                var request = (KafkaRebalance)state!;
                request.Owner.Handle(request);
            }, command);
        else Handle(command);
    }

    private void Handle(KafkaRebalance command)
    {
        try
        {
            foreach (var partition in command.Partitions)
            {
                if (command.Kind == KafkaRebalanceKind.Assigned)
                {
                    if (!_source.Assignments.ContainsKey(partition) && !_stopping) Assign(partition);
                }
                else if (_source.Assignments.TryGetValue(partition, out var assignment))
                {
                    if (command.Kind == KafkaRebalanceKind.Revoked)
                    {
                        assignment.Revoking = true;
                        _scheduler.Revoke(assignment.Token, _options.DrainTimeout);
                    }
                    else
                    {
                        _scheduler.Lose(assignment.Token);
                        _source.Assignments.Remove(partition);
                    }
                }
            }
            if (command.Kind == KafkaRebalanceKind.Revoked)
            {
                _revokes.Add(command);
                _deadline.Change(TimeSpan.Zero, TimeSpan.FromMilliseconds(5));
            }
            else command.Completion.TrySetResult();
        }
        catch (Exception error) { command.Completion.TrySetException(error); throw; }
    }

    private void ThrowFailures()
    {
        foreach (var assignment in _source.Assignments.Values)
            if (_scheduler.IsAssigned(assignment.Token) && _scheduler.GetFailure(assignment.Token) is { } failure)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(failure);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask FlushAsync(KafkaRebalance? revoke)
    {
        _source.Offsets.Clear();
        var count = 0;
        foreach (var pair in _source.Assignments)
        {
            var assignment = pair.Value;
            if (!assignment.Pending || (revoke is null ? assignment.Revoking : !revoke.Partitions.Contains(pair.Key))) continue;
            _source.Offsets.Add(assignment.Safe);
            count += checked((int)(assignment.Durable - assignment.Committed));
        }
        if (_source.Offsets.Count == 0) return;
        if (revoke?.Context is { } context)
        {
            revoke.CommitCount = count;
            ExecutionContext.Run(context, static state =>
            {
                var request = (KafkaRebalance)state!;
                request.Commit = request.Owner.CommitNativeAsync(request.CommitCount, request.Token);
            }, revoke);
            await revoke.Commit.ConfigureAwait(false);
        }
        else await CommitNativeAsync(count, revoke?.Token ?? _abort.Token).ConfigureAwait(false);
        foreach (var offset in _source.Offsets)
        {
            var assignment = _source.Assignments[new(offset.Topic, offset.Partition)];
            assignment.Committed = assignment.Durable;
            assignment.Pending = false;
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask CommitNativeAsync(int count, CancellationToken token)
    {
        using var activity = _commit.Start(new() { BatchCount = count == 0 ? null : count });
        if (count == 0) activity?.SetTag("messaging.batch.message_count", 0); // Native control-only progress has no delivered messages.
        try { await _native.CommitAsync(_source.Offsets, token).ConfigureAwait(false); }
        catch (Exception) { activity?.SetStatus(ActivityStatusCode.Error); activity?.SetTag("error.type", "_OTHER"); throw; }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask DrainAcceptedAsync()
    {
        // Shutdown takes over with its bounded drain, including handlers that outlive it.
        while (_scheduler.BufferedCount != 0 && !_stop.IsCancellationRequested)
        {
            await PumpAsync().ConfigureAwait(false);
            if (_scheduler.BufferedCount != 0) await _wake.WaitAsync().ConfigureAwait(false);
        }
    }

    // Benchmark/test entry point uses the same scheduler, ownership, snapshots and native commits.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ProcessBatchAsync(ConsumeBatch<byte[], byte[]> batch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await AdmitBatchAsync(batch).ConfigureAwait(false);
        await DrainAcceptedAsync().ConfigureAwait(false);
        return checked((int)batch.Count);
    }

    internal async ValueTask SeekAsync(TopicPartitionOffset offset, TopicPartitionTimestamp? timestamp, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset.Partition);
        if (timestamp is null) ArgumentOutOfRangeException.ThrowIfNegative(offset.Offset);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureRunning();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stop.Token);
        var request = new KafkaSeekRequest(offset, timestamp, stop.Token);
        _replay.Enqueue(request);
        await request.Completion.Task.WaitAsync(stop.Token).ConfigureAwait(false);
    }

    private async ValueTask ApplySeekAsync(KafkaSeekRequest request)
    {
        var cancellationToken = request.Token;
        cancellationToken.ThrowIfCancellationRequested();
        var offset = request.Offset;
        var partition = new TopicPartition(offset.Topic, offset.Partition);
        var ownership = _ownership.Version;
        EnsureAssigned(partition);
        if (request.Timestamp is { } search)
        {
            var offsets = await _native.GetOffsetsForTimesAsync([search], cancellationToken).ConfigureAwait(false);
            if (!offsets.TryGetValue(partition, out var found) || found < 0)
                throw new InvalidOperationException($"Kafka has no matching timestamp offset for '{partition}'. No seek was applied.");
            offset = new(offset.Topic, offset.Partition, found);
        }
        cancellationToken.ThrowIfCancellationRequested();
        if (ownership != _ownership.Version)
            throw new InvalidOperationException("Kafka assignment changed during seek. Retry against the current assignment.");
        EnsureAssigned(partition);
        _native.Seek(offset);
    }

    private void EnsureRunning()
    {
        if (_stop.IsCancellationRequested || Completion.IsCompleted)
            throw new InvalidOperationException("Kafka consumer is not running. Seek only after startup and before shutdown.");
    }

    private void EnsureAssigned(TopicPartition partition)
    {
        if (!_native.Assignment.Contains(partition))
            throw new InvalidOperationException($"Kafka partition '{partition}' is not assigned to this consumer. Replay must target its current local owner.");
    }

    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_disposed) return;
        _stop.Cancel();
        using var registration = cancellationToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _abort);
        try { await Completion.WaitAsync(cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { _abort.Cancel(); throw; }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        _stop.Cancel();
        _abort.Cancel();
        try { await Completion.ConfigureAwait(false); }
        catch (Exception) when (Completion.IsFaulted || Completion.IsCanceled) { /* Hosted service reports failures. */ }
        _scheduler.Stop(TimeSpan.Zero);
        while (_scheduler.BufferedCount != 0)
        {
            _scheduler.Pump();
            if (_scheduler.BufferedCount != 0) await _wake.WaitAsync().ConfigureAwait(false);
        }
        await _native.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
        _abort.Dispose();
        _deadline.Dispose();
    }
}
