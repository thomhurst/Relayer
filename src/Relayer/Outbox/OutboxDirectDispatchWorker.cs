using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Outbox;

// One commit/recovery dispatch state machine. Providers retain their native row buffers,
// database commands, transactional completion and failure classification.
internal abstract class OutboxDirectDispatchWorker<TBatch> : BackgroundService where TBatch : class
{
    private readonly Channel<TBatch> _queue;
    private readonly TBatch?[] _pool;
    private readonly int _queueCapacity;
    private readonly int _batchSize;
    private readonly TimeSpan _claimDuration;
    private readonly TimeProvider? _leaseTime;
    protected readonly FrozenDictionary<string, ITransport> _transports;
    private readonly DirectDispatchCapabilityCache _capabilities = new();
    protected readonly OutgoingMessage[] _messages;
    protected readonly OutboxClaim[] _claims;
    protected readonly bool[] _claimed;
    protected readonly DateTimeOffset[] _created;
    protected readonly RelayerInstrumentation? _instrumentation;
    protected readonly ILogger _logger;

    protected OutboxDirectDispatchWorker(int queueCapacity, int batchSize, TimeSpan claimDuration, IEnumerable<ITransport> transports,
        ILogger logger, RelayerInstrumentation? instrumentation, TimeProvider? leaseTime = null)
    {
        _queueCapacity = queueCapacity;
        _batchSize = batchSize;
        _claimDuration = claimDuration;
        _leaseTime = leaseTime;
        _logger = logger;
        _instrumentation = instrumentation;
        _transports = transports.ToFrozenDictionary(static transport => transport.Name, StringComparer.Ordinal);
        _queue = Channel.CreateBounded<TBatch>(new BoundedChannelOptions(queueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait
        });
        _pool = new TBatch?[queueCapacity + 1];
        _messages = new OutgoingMessage[batchSize];
        _claims = new OutboxClaim[batchSize];
        _claimed = new bool[batchSize];
        _created = new DateTimeOffset[batchSize];
    }

    protected abstract TBatch CreateBatch();
    protected abstract int Count(TBatch batch);
    protected abstract void Append(TBatch batch, TBatch source);
    protected abstract void Sort(TBatch batch);
    protected abstract void Clear(TBatch batch);
    protected abstract (string Transport, string Destination) Endpoint(TBatch batch, int offset);
    // Preserve partial progress if projecting a row throws: the caller clears every populated slot.
    protected abstract void PrepareGroup(TBatch batch, ref int offset, int limit, ref int count);
    protected abstract void LogFailure(Exception error);
    protected abstract ValueTask<int> SkipInvalidGroupAsync(TBatch batch, int offset, Exception error,
        bool recovery, CancellationToken cancellationToken);
    // Native operations are forwarded unchanged, without another async state machine on the success path.
    protected abstract ValueTask ClaimAsync(int count, DateTimeOffset now, DateTimeOffset expires, CancellationToken cancellationToken);
    protected abstract ValueTask<long> CompleteAsync(int offset, int count, CancellationToken cancellationToken);
    protected abstract ValueTask<bool> RecordSendFailureAsync(TBatch batch, int index, Exception error,
        CancellationToken cancellationToken, bool propagateDatabaseFailures, bool retryable = true);
    protected virtual void Completed(long count, bool recovery, bool single = false) => _instrumentation?.AddOutboxPending(-count);
    protected virtual bool HonorConfirmedSingle => false;
    protected virtual bool CheckCancellationBeforeRetry => true;

    protected virtual object? EnterGroup(TBatch batch, int offset, ITransport transport, out bool admitted)
    {
        admitted = true;
        return null;
    }

    protected virtual ValueTask SendLocalAsync(TBatch batch, int offset, ITransport transport, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("This dispatcher has no transactional local transport.");
    protected virtual void ExitGroup(object? admission) { }
    protected virtual bool CanDispatch(long started, TimeSpan? budget, TimeProvider? timeProvider) => true;

    protected TBatch RentBatch()
    {
        for (var i = 0; i < _pool.Length; i++)
            if (Interlocked.Exchange(ref _pool[i], null) is { } batch) return batch;
        return CreateBatch();
    }

    // Called after the provider has transferred payload ownership following durable commit.
    protected void Enqueue(TBatch batch)
    {
        var count = Count(batch);
        if (count != 0)
        {
            if (_queue.Writer.TryWrite(batch)) return;
            OutboxDirectDispatchLog.NotQueued(_logger, count);
        }
        Return(batch);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // No committing Activity or scoped service travels with a queued batch.
        try
        {
            while (await _queue.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
                while (_queue.Reader.TryRead(out var batch))
                {
                    try
                    {
                        Coalesce(batch, _queueCapacity - 1);
                        await DispatchAsync(batch, stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                    catch (Exception error) { LogFailure(error); }
                    finally { Return(batch); }
                }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally { Retire(); }
    }

    // Catalog scheduling executes one bounded pass without starting a background task.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DispatchQueuedAsync(CancellationToken cancellationToken)
    {
        for (var i = 0; i < _queueCapacity && _queue.Reader.TryRead(out var batch); i++)
        {
            try
            {
                i += Coalesce(batch, _queueCapacity - i - 1);
                await DispatchAsync(batch, cancellationToken).ConfigureAwait(false);
            }
            finally { Return(batch); }
        }
    }

    // Never wait for another commit. Bound both retained rows and notifications drained;
    // a large transaction keeps its existing within-commit dispatch behavior.
    internal int Coalesce(TBatch batch, int notificationLimit)
    {
        var drained = 0;
        while (drained < notificationLimit && Count(batch) < _batchSize &&
            _queue.Reader.TryPeek(out var next) && Count(next) <= _batchSize - Count(batch) &&
            _queue.Reader.TryRead(out next))
        {
            try { Append(batch, next); }
            finally { Return(next); }
            drained++;
        }
        return drained;
    }

    internal void Retire()
    {
        _queue.Writer.TryComplete();
        while (_queue.Reader.TryRead(out var batch)) Return(batch);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();
        try
        {
            if (ExecuteTask is { } task) await task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally { await base.StopAsync(cancellationToken).ConfigureAwait(false); }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    protected async ValueTask DispatchAsync(TBatch batch, CancellationToken cancellationToken,
        bool recovery = false, bool propagateDatabaseFailures = false, TimeSpan? admissionBudget = null,
        TimeProvider? admissionTimeProvider = null)
    {
        var started = admissionBudget.HasValue ? (admissionTimeProvider ?? TimeProvider.System).GetTimestamp() : 0;
        Sort(batch);
        var offset = 0;
        while (offset < Count(batch) && CanDispatch(started, admissionBudget, admissionTimeProvider))
        {
            var endpoint = Endpoint(batch, offset);
            ITransport transport;
            int limit;
            try
            {
                if (!_transports.TryGetValue(endpoint.Transport, out transport!))
                    throw new InvalidOperationException($"Outbox row references unregistered transport '{endpoint.Transport}'. Register its named ITransport.");
                limit = Math.Min(_batchSize, _capabilities.Get(transport, endpoint.Destination).MaxBatchSize);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                LogFailure(error);
                offset = await SkipInvalidGroupAsync(batch, offset, error, recovery, cancellationToken).ConfigureAwait(false);
                continue;
            }
            var count = 0;
            object? admission = null;
            try
            {
                var first = offset;
                PrepareGroup(batch, ref offset, limit, ref count);
                admission = EnterGroup(batch, first, transport, out var admitted);
                if (!admitted) continue;
                var now = DateTimeOffset.UtcNow;
                var expires = now + _claimDuration;
                // PostgreSQL leases use database time; measure age from before the round trip.
                var claimStarted = _leaseTime?.GetTimestamp() ?? 0;
                await ClaimAsync(count, now, expires, cancellationToken).ConfigureAwait(false);
                var live = CompactClaims(_messages.AsSpan(0, count), _claims.AsSpan(0, count), _claimed.AsSpan(0, count),
                    _created.AsSpan(0, count));
                if (live == 0) continue;
                if (_leaseTime is null ? expires <= DateTimeOffset.UtcNow : _leaseTime.GetElapsedTime(claimStarted) >= _claimDuration)
                {
                    OutboxDirectDispatchLog.ClaimExpired(_logger, live);
                    continue;
                }
                try
                {
                    if (_instrumentation is not null)
                    {
                        var dispatchTime = _leaseTime?.GetUtcNow() ?? DateTimeOffset.UtcNow;
                        for (var i = 0; i < live; i++) _instrumentation.RecordOutboxDispatchLag(dispatchTime - _created[i]);
                    }
                    if (admission is not null)
                    {
                        await SendLocalAsync(batch, first, transport, cancellationToken).ConfigureAwait(false);
                        Completed(1, recovery);
                        continue;
                    }
                    if (live == 1) await transport.SendAsync(_messages[0], cancellationToken).ConfigureAwait(false);
                    else await transport.SendBatchAsync(_messages.AsMemory(0, live), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error)
                {
                    LogFailure(error);
                    for (var i = 0; i < live; i++)
                    {
                        try
                        {
                            if ((live > 1 || HonorConfirmedSingle) && error is IBatchSendFailure result && result.IsConfirmed(i))
                            {
                                Completed(await CompleteAsync(i, 1, cancellationToken).ConfigureAwait(false), recovery, single: true);
                                continue;
                            }
                            if (live > 1 && admission is null)
                            {
                                if (CheckCancellationBeforeRetry) cancellationToken.ThrowIfCancellationRequested();
                                if (_leaseTime is null ? expires <= DateTimeOffset.UtcNow : _leaseTime.GetElapsedTime(claimStarted) >= _claimDuration) continue;
                                Exception? singleError = null;
                                try { await transport.SendAsync(_messages[i], cancellationToken).ConfigureAwait(false); }
                                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                                catch (Exception failure) { singleError = failure; }
                                if (singleError is null)
                                {
                                    // Completion failures must never be classified as failed sends.
                                    Completed(await CompleteAsync(i, 1, cancellationToken).ConfigureAwait(false), recovery, single: true);
                                    continue;
                                }
                                await RecordSendFailureAsync(batch, i, singleError, cancellationToken, propagateDatabaseFailures).ConfigureAwait(false);
                            }
                            else await RecordSendFailureAsync(batch, i, error, cancellationToken, propagateDatabaseFailures,
                                retryable: admission is null).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                        catch (Exception failure) when (!propagateDatabaseFailures) { LogFailure(failure); }
                    }
                    continue;
                }
                // Successful broker sends never consume attempts or release claims when completion fails.
                try
                {
                    var completed = await CompleteAsync(0, live, cancellationToken).ConfigureAwait(false);
                    Completed(completed, recovery);
                    if (completed != live) OutboxDirectDispatchLog.Incomplete(_logger, completed, live);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error) when (!propagateDatabaseFailures) { OutboxDirectDispatchLog.CompletionFailed(_logger, error); }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error) when (!propagateDatabaseFailures) { LogFailure(error); }
            finally
            {
                ExitGroup(admission);
                Array.Clear(_messages, 0, count);
                Array.Clear(_claims, 0, count);
            }
        }
    }

    internal static int CompactClaims(Span<OutgoingMessage> messages, Span<OutboxClaim> claims, ReadOnlySpan<bool> claimed,
        Span<DateTimeOffset> created = default)
    {
        var live = 0;
        for (var i = 0; i < claimed.Length; i++)
        {
            if (!claimed[i]) continue;
            if (!created.IsEmpty) created[live] = created[i];
            claims[live] = claims[i];
            messages[live++] = messages[i];
        }
        return live;
    }

    private void Return(TBatch batch)
    {
        Clear(batch);
        for (var i = 0; i < _pool.Length; i++)
            if (Interlocked.CompareExchange(ref _pool[i], batch, null) is null) return;
    }
}

internal static partial class OutboxDirectDispatchLog
{
    [LoggerMessage(1, LogLevel.Error, "Direct outbox dispatch queue is full or stopped. {Count} committed rows remain pending; recovery dispatch is required.")]
    internal static partial void NotQueued(ILogger logger, int count);

    [LoggerMessage(3, LogLevel.Error, "Direct outbox completion failed after a successful send. Claims remain held until expiry; recovery may duplicate delivery. Send attempts are unchanged.")]
    internal static partial void CompletionFailed(ILogger logger, Exception exception);

    [LoggerMessage(4, LogLevel.Warning, "Direct outbox completion deleted {Completed} of {Sent} sent rows. Claims expired or changed; recovery may duplicate delivery. Increase ClaimDuration if native send latency exceeds the lease.")]
    internal static partial void Incomplete(ILogger logger, long completed, int sent);

    [LoggerMessage(5, LogLevel.Warning, "Direct outbox claims expired before sending {Count} rows. Rows remain pending for recovery. Increase ClaimDuration if database latency exceeds the lease.")]
    internal static partial void ClaimExpired(ILogger logger, int count);
}
