using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed class PostgreSqlDirectDispatcher : OutboxDirectDispatchWorker<OutboxDispatchBatch>
{
    private readonly PostgreSqlOutboxStore _store;
    private readonly PostgreSqlDispatchFailures _failures;
    private readonly string _schema;
    private readonly string _prefix;
    private readonly List<BufferedOutboxMessage> _recoveryRows = [];
    private readonly OutboxDispatchBatch _recoveryBatch = new();
    private int _recoveredPoison;
    private long _recoveredSettled;
    private bool _deferSettlement;
    private readonly NpgsqlDataSource _source;
    private readonly string? _host;
    private readonly int _port;
    private readonly string? _database;
    private readonly bool _retainSentMessages;
    private readonly int _maxDispatchAttempts;
    private bool _schemaValidated;
    private int _quiescing;
    private readonly PostgreSqlOrderedDispatcher? _ordered;
    private PostgreSqlLocalStatePool<LocalAttempt>? _localAttempts;
    private TaskCompletionSource? _localDrained;
    // The queue reader owns the initial count until it exits. Each detached attempt owns one more.
    private int _localInFlight = 1;

    internal bool HasLocalTransport => _transports.Values.Any(static transport => PostgreSqlLocalTransport.Find(transport) is not null);

    internal void Quiesce()
    {
        Interlocked.Exchange(ref _quiescing, 1);
    }

    // The maintenance worker calls this once per recovery transaction. Settled rows are reported only
    // after a commit; a rollback discards the totals so a later pass cannot report them.
    internal void FinishRecovery(bool committed)
    {
        var poisoned = _recoveredPoison;
        var settled = _recoveredSettled + poisoned;
        _recoveredPoison = 0;
        _recoveredSettled = 0;

        if (!committed)
        {
            return;
        }

        if (settled != 0)
        {
            _instrumentation?.AddOutboxPending(-settled);
        }

        for (var i = 0; i < poisoned; i++)
        {
            _instrumentation?.RecordOutboxPoison();
        }
    }

    internal PostgreSqlDirectDispatcher(NpgsqlDataSource dataSource, PostgreSqlOutboxOptions storeOptions,
        PostgreSqlDirectDispatchOptions options, IEnumerable<ITransport> transports, ILogger<PostgreSqlDirectDispatcher> logger,
        PostgreSqlOutboxStore? store = null, RelayerInstrumentation? instrumentation = null, IServiceScopeFactory? scopes = null, TimeProvider? timeProvider = null,
        PostgreSqlOrderedDispatcher? ordered = null)
        : base(options.QueueCapacity, options.BatchSize, options.ClaimDuration, transports, logger, instrumentation, timeProvider ?? TimeProvider.System)
    {
        _failures = new(scopes);
        _source = dataSource;
        var target = new NpgsqlConnectionStringBuilder(dataSource.ConnectionString);
        _host = target.Host;
        _port = target.Port;
        _database = target.Database ?? target.Username;
        _retainSentMessages = storeOptions.RetainSentMessages;
        _maxDispatchAttempts = storeOptions.MaxDispatchAttempts;
        _store = store ?? new(dataSource, storeOptions);
        _schema = storeOptions.Schema ?? "public";
        _prefix = storeOptions.TablePrefix;
        // An in-process ordered worker for the same tables is woken after commit instead of waiting for its poll.
        if (ordered is not null && ordered.Matches(_schema, _prefix))
        {
            _ordered = ordered;
        }
    }

    internal void ValidateStore(string? schema, string prefix)
    {
        if (_schema != (schema ?? "public") || _prefix != prefix)
        {
            throw new InvalidOperationException("The PostgreSQL outbox and its direct dispatcher must use identical schema and table prefix settings.");
        }
    }

    internal void ValidateConnection(NpgsqlConnection writer)
    {
        var database = writer.Database;

        if (string.IsNullOrEmpty(_database) || string.IsNullOrEmpty(database))
        {
            throw new InvalidOperationException("Set Database in the PostgreSQL outbox writer and direct dispatcher connection settings when the database cannot be inferred from Username.");
        }

        if (database != _database || !MatchesEndpoint(writer.ConnectionString))
        {
            throw new InvalidOperationException("The outbox transaction and its direct dispatcher must use the same PostgreSQL host, port and database.");
        }
    }

    private bool MatchesEndpoint(string connectionString)
    {
        if (connectionString == _source.ConnectionString)
        {
            return true;
        }

        var writer = new NpgsqlConnectionStringBuilder(connectionString);
        return writer.Host == _host && writer.Port == _port;
    }

    internal void ValidateLocal(PostgreSqlLocalTransport local)
    {
        local.ValidateStore(_source, _schema, _prefix, _retainSentMessages, _maxDispatchAttempts);
    }

    internal async Task ValidateSchemaAsync(CancellationToken cancellationToken)
    {
        if (_schemaValidated)
        {
            return;
        }

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
        // Take moves unordered rows out of the source, so read ordered lanes first.
        _ordered?.Committed(rows, count);
        var batch = RentBatch();
        batch.Take(rows, count, DateTimeOffset.MaxValue);
        Enqueue(batch);
    }

    protected override void LogFailure(Exception error)
    {
        DirectDispatchLog.Failed(_logger, error);
    }

    protected override OutboxDispatchBatch CreateBatch()
    {
        return new();
    }

    protected override int Count(OutboxDispatchBatch batch)
    {
        return batch.Count;
    }

    protected override void Append(OutboxDispatchBatch batch, OutboxDispatchBatch source)
    {
        batch.Append(source);
    }

    protected override void Sort(OutboxDispatchBatch batch)
    {
        batch.Sort();
    }

    protected override void Clear(OutboxDispatchBatch batch)
    {
        batch.Clear();
    }

    protected override (string Transport, string Destination) Endpoint(OutboxDispatchBatch batch, int offset)
    {
        return (batch.Rows[offset].Transport, batch.Rows[offset].Destination);
    }

    protected override void PrepareGroup(OutboxDispatchBatch batch, ref int offset, int limit, ref int count)
    {
        var first = batch.Rows[offset];

        while (offset < batch.Count && count < limit)
        {
            var row = batch.Rows[offset];

            if (row.Transport != first.Transport || row.Destination != first.Destination)
            {
                break;
            }

            offset++;
            _claims[count] = new(row.Id, Guid.NewGuid());
            _created[count] = row.CreatedAt;
            _messages[count++] = row.ToMessage();
        }
    }

    protected override bool CanDispatch(long started, TimeSpan? budget, TimeProvider? timeProvider)
    {
        return Volatile.Read(ref _quiescing) == 0 &&
                (!budget.HasValue || (timeProvider ?? TimeProvider.System).GetElapsedTime(started) < budget.Value);
    }

    protected override ValueTask<int> SkipInvalidGroupAsync(OutboxDispatchBatch batch, int offset,
        Exception error, bool recovery, CancellationToken cancellationToken)
    {
        var first = batch.Rows[offset];
        // Skip this destination without claiming its rows or abandoning unrelated groups.
        do
        {
            offset++;
        }
        while (offset < batch.Count && batch.Rows[offset].Transport == first.Transport &&
            batch.Rows[offset].Destination == first.Destination);

        return new(offset);
    }

    protected override ValueTask ClaimAsync(int count, DateTimeOffset now, DateTimeOffset expires, CancellationToken cancellationToken)
    {
        return _store.ClaimDirectBatchAsync(_claims.AsMemory(0, count), _claimed.AsMemory(0, count), expires - now, cancellationToken);
    }

    protected override ValueTask<long> CompleteAsync(int offset, int count, CancellationToken cancellationToken)
    {
        return _store.CompleteDirectBatchAsync(_claims.AsMemory(offset, count), cancellationToken);
    }

    protected override void Completed(long count, bool recovery, bool single = false)
    {
        if (single && count != 1)
        {
            return;
        }

        if (_deferSettlement)
        {
            _recoveredSettled += count;
        }
        else
        {
            base.Completed(count, recovery, single);
        }
    }

    protected override bool HonorConfirmedSingle => true;

    protected override bool CheckCancellationBeforeRetry => false;

    protected override object? EnterGroup(OutboxDispatchBatch batch, int offset, ITransport transport, out bool admitted)
    {
        if (PostgreSqlLocalTransport.Find(transport) is not { } local)
        {
            admitted = true;
            return null;
        }

        var receiver = local.TryEnter(batch.Rows[offset].Destination);
        admitted = receiver is not null;
        return receiver;
    }

    protected override ValueTask SendLocalAsync(OutboxDispatchBatch batch, int offset, ITransport transport, CancellationToken cancellationToken)
    {
        var local = PostgreSqlLocalTransport.Find(transport)!;
        ValidateLocal(local);
        return local.ConsumeAsync(_messages[0], _claims[0], batch.Rows[offset].Attempts + 1, _store.DispatchTransaction, cancellationToken);
    }

    protected override bool TryDispatchLocalInBackground(OutboxDispatchBatch batch, int offset, ITransport transport,
        object admission, CancellationToken cancellationToken)
    {
        var receiver = (PostgreSqlLocalTransport.Receiver)admission;

        if (!receiver.ConcurrentDirectDispatch)
        {
            return false;
        }

        var local = PostgreSqlLocalTransport.Find(transport)!;
        ValidateLocal(local);
        _localDrained ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = _localAttempts ??= new(2, () => new LocalAttempt(this));
        var attempt = attempts.Rent();
        Interlocked.Increment(ref _localInFlight);
        attempt.Start(batch, offset, local, receiver, _messages[0], _claims[0], cancellationToken);
        return true;
    }

    protected override Task DrainLocalDispatchAsync()
    {
        if (_localDrained is not { } drained)
        {
            return Task.CompletedTask;
        }

        LocalAttemptFinished();
        return drained.Task;
    }

    private void LocalAttemptFinished()
    {
        if (Interlocked.Decrement(ref _localInFlight) == 0)
        {
            _localDrained!.SetResult();
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask DispatchLocalAttemptAsync(PostgreSqlLocalTransport local, BufferedOutboxMessage row,
        OutgoingMessage message, OutboxClaim claim, CancellationToken cancellationToken)
    {
        try
        {
            await local.ConsumeAsync(message, claim, row.Attempts + 1, null, cancellationToken).ConfigureAwait(false);
            base.Completed(1, recovery: false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception error)
        {
            if (error is not Relayer.Pipeline.ConsumeFailureHandoff)
            {
                LogFailure(error);
            }

            var store = local.RentCompletionStore();

            try
            {
                await RecordFailureAsync(store, row, message, claim, error, cancellationToken,
                    propagateDatabaseFailures: false, deferSettlement: false, retryable: false).ConfigureAwait(false);
            }
            finally
            {
                local.ReturnCompletionStore(store);
            }
        }
    }

    private sealed class LocalAttempt
    {
        private readonly PostgreSqlDirectDispatcher _owner;
        private readonly Action _finish;
        private BufferedOutboxMessage _row = new();
        private PostgreSqlLocalTransport.Receiver? _receiver;
        private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _completion;

        internal LocalAttempt(PostgreSqlDirectDispatcher owner)
        {
            _owner = owner;
            _finish = Finish;
        }

        internal void Start(OutboxDispatchBatch batch, int offset, PostgreSqlLocalTransport local,
            PostgreSqlLocalTransport.Receiver receiver, OutgoingMessage message, OutboxClaim claim,
            CancellationToken cancellationToken)
        {
            _receiver = receiver;
            _row = batch.ExchangeRow(offset, _row);
            var completion = _owner.DispatchLocalAttemptAsync(local, _row, message, claim, cancellationToken)
                .ConfigureAwait(false).GetAwaiter();
            _completion = completion;
            // Consume the pooled ValueTask exactly once, keeping the borrowed payload alive through settlement.
            if (completion.IsCompleted)
            {
                Finish();
            }
            else
            {
                completion.UnsafeOnCompleted(_finish);
            }
        }

        private void Finish()
        {
            try
            {
                _completion.GetResult();
            }
            catch (Exception error)
            {
                _owner.LogFailure(error);
            }
            finally
            {
                _completion = default;
                _row.Clear();
                _receiver!.Exit();
                _receiver = null;
                _owner._localAttempts!.Return(this);
                _owner.LocalAttemptFinished();
            }
        }
    }

    protected override void ExitGroup(object? admission)
    {
        ((PostgreSqlLocalTransport.Receiver?)admission)?.Exit();
    }

    // Recovery dispatches these rows only after its pass commits: their claim must commit before the handler runs.
    internal bool IsAtLeastOnce(in OutboxRecord row)
    {
        return _transports.TryGetValue(row.TransportName, out var transport)
                && PostgreSqlLocalTransport.Find(transport)?.GetReceiver(row.Message.Destination)?.AtLeastOnce == true;
    }

    protected override ValueTask<bool> RecordSendFailureAsync(OutboxDispatchBatch batch, int index, Exception error,
        CancellationToken cancellationToken, bool propagateDatabaseFailures, bool retryable = true)
    {
        BufferedOutboxMessage? row = null;

        for (var i = 0; i < batch.Count; i++)
        {
            if (batch.Rows[i].Id == _claims[index].MessageId)
            {
                row = batch.Rows[i];
                break;
            }
        }

        if (row is null)
        {
            throw new InvalidOperationException("The failed claim has no dispatch row.");
        }

        return RecordFailureAsync(_store, row, _messages[index], _claims[index], error, cancellationToken,
            propagateDatabaseFailures, _deferSettlement, retryable);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> RecordFailureAsync(PostgreSqlOutboxStore store, BufferedOutboxMessage row,
        OutgoingMessage message, OutboxClaim claim, Exception error, CancellationToken cancellationToken,
        bool propagateDatabaseFailures, bool deferSettlement, bool retryable)
    {
        // A failed at-least-once completion is not a failed attempt: the claim expires and the row is delivered again.
        // Recovery still surfaces its database error, so change capture never checkpoints past it.
        if (error is PostgreSqlLocalCompletionFailure completion)
        {
            if (propagateDatabaseFailures && completion.InnerException is { } database)
            {
                ExceptionDispatchInfo.Throw(database);
            }

            return false;
        }

        var action = await _failures.ClassifyAsync(error, row.Id, row.MessageType, row.Attempts + 1,
            row.TenantId, message, cancellationToken).ConfigureAwait(false);

        if (action == FailureAction.Discard)
        {
            var completed = await store.CompleteDirectAsync(claim, cancellationToken).ConfigureAwait(false);

            if (deferSettlement)
            {
                _recoveredSettled += completed;
            }
            else
            {
                _instrumentation?.AddOutboxPending(-completed);
            }

            return completed != 0;
        }

        if (action is not (FailureAction.Retry or FailureAction.DeadLetter))
        {
            throw new InvalidOperationException("The dispatch failure classifier returned an unknown action.");
        }
        // Only a local attempt is non-retryable here; its Retry follows the consumer's schedule.
        var consumer = !retryable && action == FailureAction.Retry && _transports.TryGetValue(row.Transport, out var transport)
            ? PostgreSqlLocalTransport.Find(transport)?.GetReceiver(row.Destination) : null;
        var result = consumer is not null
            ? await store.RecordLocalFailureAsync(claim, PostgreSqlDispatchFailures.Describe(error), consumer.MaxAttempts,
                consumer.RedeliveryDelay(row.Attempts + 1), cancellationToken).ConfigureAwait(false)
            : await store.RecordDispatchFailureAsync(claim, PostgreSqlDispatchFailures.Describe(error), null,
                cancellationToken, permanent: action == FailureAction.DeadLetter, retryable: retryable).ConfigureAwait(false);
        // Unordered exhaustion moves the row to dead storage. Recovery records it once its transaction commits.
        if (result == 2)
        {
            if (propagateDatabaseFailures)
            {
                _recoveredPoison++;
            }
            else if (_instrumentation is not null)
            {
                _instrumentation.AddOutboxPending(-1);
                _instrumentation.RecordOutboxPoison();
            }
        }

        return true;
    }

    // Recovery owns a separate dispatcher instance, so these buffers never overlap the commit queue.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DispatchRecoveredAsync(ReadOnlyMemory<OutboxRecord> rows, CancellationToken cancellationToken,
        bool propagateDatabaseFailures = false, TimeSpan? admissionBudget = null, TimeProvider? admissionTimeProvider = null)
    {
        _recoveredPoison = 0;
        _recoveredSettled = 0;
        _deferSettlement = propagateDatabaseFailures;
        var batch = PrepareRecovered(rows.Span);
        // An aborted PostgreSQL transaction can accept COMMIT as ROLLBACK. Recovery must observe
        // database errors before its caller commits or advances the replication checkpoint.
        try
        {
            await DispatchAsync(batch, cancellationToken, recovery: true, propagateDatabaseFailures, admissionBudget, admissionTimeProvider).ConfigureAwait(false);
        }
        finally
        {
            _deferSettlement = false;
            batch.Clear();
        }
    }

    internal OutboxDispatchBatch PrepareRecovered(ReadOnlySpan<OutboxRecord> rows)
    {
        for (var i = 0; i < rows.Length; i++)
        {
            if (_recoveryRows.Count == i)
            {
                _recoveryRows.Add(new());
            }

            _recoveryRows[i].Copy(rows[i], 1);
        }

        _recoveryBatch.Take(_recoveryRows, rows.Length, DateTimeOffset.MaxValue);
        return _recoveryBatch;
    }
}

internal static partial class DirectDispatchLog
{
    [LoggerMessage(2, LogLevel.Error, "Direct outbox dispatch failed. Durable rows remain available for recovery or, after attempt exhaustion, dead-letter management.")]
    internal static partial void Failed(ILogger logger, Exception exception);
}
