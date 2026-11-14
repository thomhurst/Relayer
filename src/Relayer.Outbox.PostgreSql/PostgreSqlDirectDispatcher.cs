using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
        PostgreSqlOutboxStore? store = null, RelayerInstrumentation? instrumentation = null, IServiceScopeFactory? scopes = null, TimeProvider? timeProvider = null)
        : base(options.QueueCapacity, options.BatchSize, options.ClaimDuration, transports, logger, instrumentation, timeProvider ?? TimeProvider.System)
    {
        _failures = new(scopes);
        _source = dataSource;
        _retainSentMessages = storeOptions.RetainSentMessages;
        _maxDispatchAttempts = storeOptions.MaxDispatchAttempts;
        _store = store ?? new(dataSource, storeOptions);
        _schema = storeOptions.Schema ?? "public";
        _prefix = storeOptions.TablePrefix;
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
        var batch = RentBatch();
        batch.Take(rows, count, DateTimeOffset.MaxValue);
        Enqueue(batch);
    }

    protected override void LogFailure(Exception error) => DirectDispatchLog.Failed(_logger, error);

    protected override OutboxDispatchBatch CreateBatch() => new();
    protected override int Count(OutboxDispatchBatch batch) => batch.Count;
    protected override void Append(OutboxDispatchBatch batch, OutboxDispatchBatch source) => batch.Append(source);
    protected override void Sort(OutboxDispatchBatch batch) => batch.Sort();
    protected override void Clear(OutboxDispatchBatch batch) => batch.Clear();
    protected override (string Transport, string Destination) Endpoint(OutboxDispatchBatch batch, int offset) =>
        (batch.Rows[offset].Transport, batch.Rows[offset].Destination);

    protected override void PrepareGroup(OutboxDispatchBatch batch, ref int offset, int limit, ref int count)
    {
        var first = batch.Rows[offset];
        while (offset < batch.Count && count < limit)
        {
            var row = batch.Rows[offset];
            if (row.Transport != first.Transport || row.Destination != first.Destination) break;
            offset++;
            _claims[count] = new(row.Id, Guid.NewGuid());
            _created[count] = row.CreatedAt;
            _messages[count++] = row.ToMessage();
        }
    }

    protected override bool CanDispatch(long started, TimeSpan? budget, TimeProvider? timeProvider) =>
        Volatile.Read(ref _quiescing) == 0 &&
        (!budget.HasValue || (timeProvider ?? TimeProvider.System).GetElapsedTime(started) < budget.Value);

    protected override ValueTask<int> SkipInvalidGroupAsync(OutboxDispatchBatch batch, int offset,
        Exception error, bool recovery, CancellationToken cancellationToken)
    {
        var first = batch.Rows[offset];
        // Skip this destination without claiming its rows or abandoning unrelated groups.
        do { offset++; }
        while (offset < batch.Count && batch.Rows[offset].Transport == first.Transport &&
            batch.Rows[offset].Destination == first.Destination);
        return new(offset);
    }

    protected override ValueTask ClaimAsync(int count, DateTimeOffset now, DateTimeOffset expires, CancellationToken cancellationToken) =>
        _store.ClaimDirectBatchAsync(_claims.AsMemory(0, count), _claimed.AsMemory(0, count), expires - now, cancellationToken);

    protected override ValueTask<long> CompleteAsync(int offset, int count, CancellationToken cancellationToken) =>
        _store.CompleteDirectBatchAsync(_claims.AsMemory(offset, count), cancellationToken);

    protected override void Completed(long count, bool recovery, bool single = false)
    {
        if (!single || count == 1) base.Completed(count, recovery, single);
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

    protected override void ExitGroup(object? admission) => ((PostgreSqlLocalTransport.Receiver?)admission)?.Exit();

    protected override async ValueTask<bool> RecordSendFailureAsync(OutboxDispatchBatch batch, int index, Exception error,
        CancellationToken cancellationToken, bool propagateDatabaseFailures, bool retryable = true)
    {
        BufferedOutboxMessage? row = null;
        for (var i = 0; i < batch.Count; i++)
            if (batch.Rows[i].Id == _claims[index].MessageId) { row = batch.Rows[i]; break; }
        if (row is null) throw new InvalidOperationException("The failed claim has no dispatch row.");
        var action = await _failures.ClassifyAsync(error, row.Id, row.MessageType, row.Attempts + 1,
            row.TenantId, _messages[index], cancellationToken).ConfigureAwait(false);
        if (action == FailureAction.Discard)
        {
            var completed = await _store.CompleteDirectBatchAsync(_claims.AsMemory(index, 1), cancellationToken).ConfigureAwait(false);
            _instrumentation?.AddOutboxPending(-completed);
            return completed != 0;
        }
        if (action is not (FailureAction.Retry or FailureAction.DeadLetter))
            throw new InvalidOperationException("The dispatch failure classifier returned an unknown action.");
        if (await _store.RecordDispatchFailureAsync(_claims[index], error.Message, null,
            cancellationToken, permanent: action == FailureAction.DeadLetter, retryable: retryable).ConfigureAwait(false) == 2)
        {
            if (propagateDatabaseFailures) _recoveredPoison++;
            else _instrumentation?.RecordOutboxPoison();
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
        var batch = PrepareRecovered(rows.Span);
        // An aborted PostgreSQL transaction can accept COMMIT as ROLLBACK. Recovery must observe
        // database errors before its caller commits or advances the replication checkpoint.
        try { await DispatchAsync(batch, cancellationToken, recovery: true, propagateDatabaseFailures, admissionBudget, admissionTimeProvider).ConfigureAwait(false); }
        finally { batch.Clear(); }
    }

    internal OutboxDispatchBatch PrepareRecovered(ReadOnlySpan<OutboxRecord> rows)
    {
        for (var i = 0; i < rows.Length; i++)
        {
            if (_recoveryRows.Count == i) _recoveryRows.Add(new());
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
