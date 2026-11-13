using System.Collections.Frozen;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed class PostgreSqlDirectDispatcher : BackgroundService
{
    private readonly PostgreSqlOutboxStore _store;
    private readonly PostgreSqlDispatchFailures _failures;
    private readonly PostgreSqlDirectDispatchOptions _options;
    private readonly ILogger<PostgreSqlDirectDispatcher> _logger;
    private readonly Channel<OutboxDispatchBatch> _queue;
    private readonly OutboxDispatchBatch?[] _pool;
    private readonly FrozenDictionary<string, ITransport> _transports;
    private readonly DirectDispatchCapabilityCache _capabilities = new();
    private readonly OutgoingMessage[] _messages;
    private readonly OutboxClaim[] _claims;
    private readonly bool[] _claimed;
    private readonly DateTimeOffset[] _created;
    private readonly RelayerInstrumentation? _instrumentation;
    private readonly string _schema;
    private readonly string _prefix;
    private readonly List<BufferedOutboxMessage> _recoveryRows = [];
    private readonly OutboxDispatchBatch _recoveryBatch = new();
    private int _recoveredPoison;
    private readonly Npgsql.NpgsqlDataSource _source;
    private readonly bool _retainSentMessages;
    private readonly int _maxDispatchAttempts;
    private bool _schemaValidated;
    private int _quiescing;
    internal bool HasLocalTransport => _transports.Values.Any(static transport => PostgreSqlLocalTransport.Find(transport) is not null);
    internal void Quiesce() => Interlocked.Exchange(ref _quiescing, 1);

    internal void RecoveryCommitted()
    {
        for (var i = 0; i < _recoveredPoison; i++) _instrumentation?.RecordOutboxPoison();
        _recoveredPoison = 0;
    }

    internal PostgreSqlDirectDispatcher(Npgsql.NpgsqlDataSource dataSource, PostgreSqlOutboxOptions storeOptions,
        PostgreSqlDirectDispatchOptions options, IEnumerable<ITransport> transports, ILogger<PostgreSqlDirectDispatcher> logger,
        PostgreSqlOutboxStore? store = null, RelayerInstrumentation? instrumentation = null, IServiceScopeFactory? scopes = null)
    {
        _failures = new(scopes);
        _source = dataSource;
        _retainSentMessages = storeOptions.RetainSentMessages;
        _maxDispatchAttempts = storeOptions.MaxDispatchAttempts;
        _store = store ?? new(dataSource, storeOptions);
        _schema = storeOptions.Schema ?? "public";
        _prefix = storeOptions.TablePrefix;
        _options = options;
        _instrumentation = instrumentation;
        _logger = logger;
        _transports = transports.ToFrozenDictionary(static transport => transport.Name, StringComparer.Ordinal);
        _queue = Channel.CreateBounded<OutboxDispatchBatch>(new BoundedChannelOptions(options.QueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait
        });
        _pool = new OutboxDispatchBatch?[options.QueueCapacity + 1];
        _messages = new OutgoingMessage[options.BatchSize];
        _claims = new OutboxClaim[options.BatchSize];
        _claimed = new bool[options.BatchSize];
        _created = new DateTimeOffset[options.BatchSize];
    }

    internal void ValidateStore(string? schema, string prefix)
    {
        if (_schema != (schema ?? "public") || _prefix != prefix)
            throw new InvalidOperationException("The EF Core outbox and its direct dispatcher must use identical PostgreSQL schema and table prefix settings.");
    }

    internal void ValidateLocal(PostgreSqlLocalTransport local) =>
        local.ValidateStore(_source, _schema, _prefix, _retainSentMessages, _maxDispatchAttempts);

    internal async Task ValidateSchemaAsync(CancellationToken cancellationToken)
    {
        if (_schemaValidated) return;
        await PostgreSqlSentSchema.ValidateAsync(_source, _retainSentMessages, _schema, _prefix, cancellationToken).ConfigureAwait(false);
        _schemaValidated = true;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await ValidateSchemaAsync(cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    // Called only after durable commit. Queue pressure and shutdown cannot make a committed transaction fail.
    internal void Committed(List<BufferedOutboxMessage> rows, int count)
    {
        OutboxDispatchBatch? batch = null;
        for (var i = 0; i < _pool.Length; i++)
            if ((batch = Interlocked.Exchange(ref _pool[i], null)) is not null) break;
        batch ??= new();
        batch.Take(rows, count, DateTimeOffset.UtcNow);
        if (batch.Count == 0) { Return(batch); return; }
        if (_queue.Writer.TryWrite(batch)) return;
        DirectDispatchLog.NotQueued(_logger, batch.Count);
        Return(batch);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // The queue carries payloads alone: no committing Activity or scoped service travels with a batch.
        try
        {
            while (await _queue.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
                while (_queue.Reader.TryRead(out var batch))
                {
                    try { await DispatchAsync(batch, stoppingToken).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { return; }
                    catch (Exception error) { DirectDispatchLog.Failed(_logger, error); }
                    finally { Return(batch); }
                }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            _queue.Writer.TryComplete();
            while (_queue.Reader.TryRead(out var batch)) Return(batch);
        }
    }

    // Catalog scheduling owns execution; it never starts a background task for this store.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DispatchQueuedAsync(CancellationToken cancellationToken)
    {
        // One bounded pass: producers cannot keep a tenant on a worker indefinitely.
        for (var i = 0; i < _options.QueueCapacity && _queue.Reader.TryRead(out var batch); i++)
        {
            try { await DispatchAsync(batch, cancellationToken).ConfigureAwait(false); }
            finally { Return(batch); }
        }
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
    private async ValueTask DispatchAsync(OutboxDispatchBatch batch, CancellationToken cancellationToken,
        bool propagateDatabaseFailures = false, TimeSpan? admissionBudget = null, TimeProvider? admissionTimeProvider = null)
    {
        var admissionStarted = admissionBudget.HasValue ? (admissionTimeProvider?.GetTimestamp() ?? Stopwatch.GetTimestamp()) : 0;
        batch.Sort();
        var offset = 0;
        while (offset < batch.Count && Volatile.Read(ref _quiescing) == 0)
        {
            if (admissionBudget is { } budget && (admissionTimeProvider?.GetElapsedTime(admissionStarted) ?? Stopwatch.GetElapsedTime(admissionStarted)) >= budget) break;
            var first = batch.Rows[offset];
            ITransport transport;
            int limit;
            try
            {
                if (!_transports.TryGetValue(first.Transport, out transport!))
                    throw new InvalidOperationException($"Outbox row references unregistered transport '{first.Transport}'. Register its named ITransport.");
                var capabilities = _capabilities.Get(transport, first.Destination);
                limit = Math.Min(_options.BatchSize, capabilities.MaxBatchSize);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                DirectDispatchLog.Failed(_logger, error);
                // Skip this destination without claiming its rows or abandoning unrelated groups.
                do { offset++; }
                while (offset < batch.Count && batch.Rows[offset].Transport == first.Transport &&
                    batch.Rows[offset].Destination == first.Destination);
                continue;
            }
            var count = 0;
            PostgreSqlLocalTransport.Receiver? localAdmission = null;
            try
            {
                while (offset < batch.Count && count < limit)
                {
                    var row = batch.Rows[offset];
                    if (row.Transport != first.Transport || row.Destination != first.Destination) break;
                    offset++;
                    _claims[count] = new(row.Id, Guid.NewGuid());
                    _created[count] = row.CreatedAt;
                    _messages[count++] = row.ToMessage();
                }
                if (PostgreSqlLocalTransport.Find(transport) is { } localTransport)
                {
                    localAdmission = localTransport.TryEnter(first.Destination);
                    if (localAdmission is null) continue; // Durable rows remain available for a later recovery pass.
                }
                var now = DateTimeOffset.UtcNow;
                var expires = now + _options.ClaimDuration;
                await _store.ClaimBatchAsync(_claims.AsMemory(0, count), _claimed.AsMemory(0, count), now, expires, cancellationToken).ConfigureAwait(false);
                var live = CompactClaims(_messages.AsSpan(0, count), _claims.AsSpan(0, count), _claimed.AsSpan(0, count),
                    _created.AsSpan(0, count));
                if (live == 0) continue;
                // A slow database response can consume the lease. Never start a send with known expired ownership.
                if (expires <= DateTimeOffset.UtcNow)
                {
                    DirectDispatchLog.ClaimExpired(_logger, live);
                    continue;
                }
                try
                {
                    if (_instrumentation is not null)
                    {
                        var dispatchTime = DateTimeOffset.UtcNow;
                        for (var i = 0; i < live; i++) _instrumentation.RecordOutboxDispatchLag(dispatchTime - _created[i]);
                    }
                    if (PostgreSqlLocalTransport.Find(transport) is { } local)
                    {
                        ValidateLocal(local);
                        await local.ConsumeAsync(_messages[0], _claims[0], first.Attempts + 1, _store.DispatchTransaction, cancellationToken).ConfigureAwait(false);
                        _instrumentation?.AddOutboxPending(-1);
                        continue;
                    }
                    if (live == 1) await transport.SendAsync(_messages[0], cancellationToken).ConfigureAwait(false);
                    else await transport.SendBatchAsync(_messages.AsMemory(0, live), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error)
                {
                    DirectDispatchLog.Failed(_logger, error);
                    Exception? retryError = null;
                    for (var i = 0; i < live; i++)
                    {
                        try
                        {
                            // Native batches may reject every row because one row is invalid. Retry each
                            // individually under the same live claim; at-least-once delivery permits duplicates.
                            if (retryError is not null)
                            {
                                await RecordSendFailureAsync(batch, i, retryError, cancellationToken, propagateDatabaseFailures, FailureAction.Retry).ConfigureAwait(false);
                            }
                            else if (live > 1 && PostgreSqlLocalTransport.Find(transport) is null)
                            {
                                if (expires <= DateTimeOffset.UtcNow)
                                {
                                    // Stop native sends, but still attempt fenced backoff for every remaining
                                    // claim. Expired or replaced ownership remains protected by the store.
                                    retryError = error;
                                    await RecordSendFailureAsync(batch, i, error, cancellationToken, propagateDatabaseFailures, FailureAction.Retry).ConfigureAwait(false);
                                    continue;
                                }
                                Exception? singleError = null;
                                try { await transport.SendAsync(_messages[i], cancellationToken).ConfigureAwait(false); }
                                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                                catch (Exception failure) { singleError = failure; }
                                if (singleError is null)
                                {
                                    // Completion failures must never be classified as failed sends.
                                    if (await _store.CompleteBatchAsync(_claims.AsMemory(i, 1), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false) == 1)
                                        _instrumentation?.AddOutboxPending(-1);
                                    continue;
                                }
                                if (await RecordSendFailureAsync(batch, i, singleError, cancellationToken, propagateDatabaseFailures).ConfigureAwait(false) == FailureAction.Retry)
                                    retryError = singleError; // A retryable failure ends this isolation pass.
                            }
                            else await RecordSendFailureAsync(batch, i, error, cancellationToken, propagateDatabaseFailures,
                                retryable: PostgreSqlLocalTransport.Find(transport) is null).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                        catch (Exception failure) when (!propagateDatabaseFailures) { DirectDispatchLog.Failed(_logger, failure); }
                    }
                    continue;
                }
                // Once the broker succeeds, database errors must not consume send attempts or release claims.
                try
                {
                    var completed = await _store.CompleteBatchAsync(_claims.AsMemory(0, live), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
                    _instrumentation?.AddOutboxPending(-completed);
                    if (completed != live) DirectDispatchLog.Incomplete(_logger, completed, live);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error) when (!propagateDatabaseFailures) { DirectDispatchLog.CompletionFailed(_logger, error); }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error) when (!propagateDatabaseFailures) { DirectDispatchLog.Failed(_logger, error); }
            finally
            {
                localAdmission?.Exit();
                Array.Clear(_messages, 0, count);
                Array.Clear(_claims, 0, count);
            }
        }
    }

    private async ValueTask<FailureAction> RecordSendFailureAsync(OutboxDispatchBatch batch, int index, Exception error,
        CancellationToken cancellationToken, bool propagateDatabaseFailures, FailureAction? knownAction = null, bool retryable = true)
    {
        BufferedOutboxMessage? row = null;
        for (var i = 0; i < batch.Count; i++)
            if (batch.Rows[i].Id == _claims[index].MessageId) { row = batch.Rows[i]; break; }
        if (row is null) throw new InvalidOperationException("The failed claim has no dispatch row.");
        var action = knownAction ?? await _failures.ClassifyAsync(error, row.Id, row.MessageType, row.Attempts + 1,
            row.TenantId, _messages[index], cancellationToken).ConfigureAwait(false);
        if (action == FailureAction.Discard)
        {
            var completed = await _store.CompleteBatchAsync(_claims.AsMemory(index, 1), DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
            _instrumentation?.AddOutboxPending(-completed);
            return action;
        }
        if (action is not (FailureAction.Retry or FailureAction.DeadLetter))
            throw new InvalidOperationException("The dispatch failure classifier returned an unknown action.");
        if (await _store.RecordDispatchFailureAsync(_claims[index], error.Message, DateTimeOffset.UtcNow,
            cancellationToken, permanent: action == FailureAction.DeadLetter, retryable: retryable).ConfigureAwait(false) == 2)
        {
            if (propagateDatabaseFailures) _recoveredPoison++;
            else _instrumentation?.RecordOutboxPoison();
        }
        return action;
    }

    // Recovery owns a separate dispatcher instance, so these buffers never overlap the commit queue.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DispatchRecoveredAsync(ReadOnlyMemory<OutboxRecord> rows, CancellationToken cancellationToken,
        bool propagateDatabaseFailures = false, TimeSpan? admissionBudget = null, TimeProvider? admissionTimeProvider = null)
    {
        _recoveredPoison = 0;
        var batch = PrepareRecovered(rows.Span);
        // An aborted PostgreSQL transaction can accept COMMIT as ROLLBACK. Recovery must observe
        // database errors before its caller commits or advances the replication checkpoint.
        try { await DispatchAsync(batch, cancellationToken, propagateDatabaseFailures, admissionBudget, admissionTimeProvider).ConfigureAwait(false); }
        finally { batch.Clear(); }
    }

    internal OutboxDispatchBatch PrepareRecovered(ReadOnlySpan<OutboxRecord> rows)
    {
        for (var i = 0; i < rows.Length; i++)
        {
            if (_recoveryRows.Count == i) _recoveryRows.Add(new());
            _recoveryRows[i].Copy(rows[i], 1);
        }
        _recoveryBatch.Take(_recoveryRows, rows.Length, DateTimeOffset.UtcNow);
        return _recoveryBatch;
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

    private void Return(OutboxDispatchBatch batch)
    {
        batch.Clear();
        for (var i = 0; i < _pool.Length; i++)
            if (Interlocked.CompareExchange(ref _pool[i], batch, null) is null) return;
    }
}

internal static partial class DirectDispatchLog
{
    [LoggerMessage(1, LogLevel.Error, "Direct outbox dispatch queue is full or stopped. {Count} committed rows remain pending; recovery dispatch is required.")]
    internal static partial void NotQueued(ILogger logger, int count);

    [LoggerMessage(2, LogLevel.Error, "Direct outbox dispatch failed. Durable rows remain available for recovery or, after attempt exhaustion, dead-letter management.")]
    internal static partial void Failed(ILogger logger, Exception exception);

    [LoggerMessage(3, LogLevel.Error, "Direct outbox completion failed after a successful send. Claims remain held until expiry; recovery may duplicate delivery. Send attempts are unchanged.")]
    internal static partial void CompletionFailed(ILogger logger, Exception exception);

    [LoggerMessage(4, LogLevel.Warning, "Direct outbox completion deleted {Completed} of {Sent} sent rows. Claims expired or changed; recovery may duplicate delivery. Increase ClaimDuration if native send latency exceeds the lease.")]
    internal static partial void Incomplete(ILogger logger, long completed, int sent);

    [LoggerMessage(5, LogLevel.Warning, "Direct outbox claims expired before sending {Count} rows. Rows remain pending for recovery. Increase ClaimDuration if database latency exceeds the lease.")]
    internal static partial void ClaimExpired(ILogger logger, int count);
}
