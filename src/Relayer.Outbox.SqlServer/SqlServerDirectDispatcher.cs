using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer;

internal sealed class SqlServerDirectDispatcher : OutboxDirectDispatchWorker<OutboxDispatchBatch>
{
    private readonly SqlServerOutboxStore _store;
    private readonly Func<Exception, bool>? _retrySendFailure;
    private readonly SqlServerDirectDispatchOptions _options;
    private long _recoveredCompletions;
    private readonly string _schema;
    private readonly string _prefix;
    private readonly string _dataSource;
    private readonly string _database;

    [RequiresUnreferencedCode("SqlClient runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    internal SqlServerDirectDispatcher(string connectionString, SqlServerOutboxOptions storeOptions,
        SqlServerDirectDispatchOptions options, IEnumerable<ITransport> transports, ILogger<SqlServerDirectDispatcher> logger,
        SqlServerOutboxStore? store = null, RelayerInstrumentation? instrumentation = null)
        : base(options.QueueCapacity, options.BatchSize, options.ClaimDuration, transports, logger, instrumentation)
    {
        _store = store ?? new(connectionString, storeOptions);
        _retrySendFailure = storeOptions.RetrySendFailure;
        var target = ValidateConnectionString(connectionString);
        _dataSource = target.DataSource;
        _database = target.InitialCatalog;
        _schema = storeOptions.Schema ?? "dbo";
        _prefix = storeOptions.TablePrefix;
        _options = options;
    }

    internal void ValidateStore(string? schema, string prefix)
    {
        if (_schema != (schema ?? "dbo") || _prefix != prefix)
            throw new InvalidOperationException("The SQL Server outbox and its direct dispatcher must use identical schema and table prefix settings.");
    }

    internal static SqlConnectionStringBuilder ValidateConnectionString(string connectionString)
    {
        var target = new SqlConnectionStringBuilder(connectionString);
        if (string.IsNullOrWhiteSpace(target.InitialCatalog))
            throw new ArgumentException("SQL Server direct dispatch requires an explicit Initial Catalog (Database) in its connection string so writer and dispatcher targets can be validated without opening a second pooled connection.", nameof(connectionString));
        return target;
    }

    internal void ValidateConnection(SqlConnection writer)
    {
        if (!StringComparer.OrdinalIgnoreCase.Equals(writer.DataSource, _dataSource) ||
            !StringComparer.Ordinal.Equals(writer.Database, _database))
            throw new InvalidOperationException("The SQL Server outbox transaction and its direct dispatcher must use the same server and database.");
    }

    // Called only after durable commit. Queue pressure and shutdown cannot make a committed transaction fail.
    internal void Committed(List<SqlServerBufferedMessage> rows, int count)
    {
        var batch = RentBatch();
        batch.Take(rows, count, DateTimeOffset.UtcNow);
        Enqueue(batch);
    }

    protected override void LogFailure(Exception error) => DirectDispatchLog.Failed(_logger, error);

    protected override OutboxDispatchBatch CreateBatch() => new();
    protected override int Count(OutboxDispatchBatch batch) => batch.Count;
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

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    protected override async ValueTask<int> SkipInvalidGroupAsync(OutboxDispatchBatch batch, int offset,
        Exception error, bool recovery, CancellationToken cancellationToken)
    {
        var first = batch.Rows[offset];
        // Fence and record invalid destinations independently, then continue unrelated groups.
        do
        {
            var id = batch.Rows[offset++].Id;
            if (!recovery) continue;
            var claim = new OutboxClaim(id, Guid.NewGuid());
            var now = DateTimeOffset.UtcNow;
            if (await _store.TryClaimAsync(claim, now, now + _options.ClaimDuration, cancellationToken).ConfigureAwait(false))
                await _store.RecordFailureAsync(claim, error.Message, DateTimeOffset.UtcNow, cancellationToken).ConfigureAwait(false);
        }
        while (offset < batch.Count && batch.Rows[offset].Transport == first.Transport &&
            batch.Rows[offset].Destination == first.Destination);
        return offset;
    }

    protected override ValueTask ClaimAsync(int count, DateTimeOffset now, DateTimeOffset expires, CancellationToken cancellationToken) =>
        _store.ClaimBatchAsync(_claims.AsMemory(0, count), _claimed.AsMemory(0, count), now, expires, cancellationToken);

    protected override ValueTask<long> CompleteAsync(int offset, int count, CancellationToken cancellationToken) =>
        _store.CompleteBatchAsync(_claims.AsMemory(offset, count), DateTimeOffset.UtcNow, cancellationToken);

    protected override ValueTask<bool> RecordSendFailureAsync(OutboxDispatchBatch batch, int index, Exception error,
        CancellationToken cancellationToken, bool propagateDatabaseFailures, bool retryable = true) =>
        SqlServerSendFailure.IsRetryable(_retrySendFailure, error, _logger)
            ? _store.RecordRetryAsync(_claims[index], error.Message, DateTimeOffset.UtcNow, cancellationToken)
            : _store.RecordFailureAsync(_claims[index], error.Message, DateTimeOffset.UtcNow, cancellationToken);

    protected override void Completed(long count, bool recovery, bool single = false)
    {
        if (recovery) _recoveredCompletions += count;
        else base.Completed(count, recovery, single);
    }

    // The maintenance worker owns this dispatcher and calls this once per transaction.
    internal void FinishRecovery(bool committed)
    {
        var completed = _recoveredCompletions;
        _recoveredCompletions = 0;
        if (committed && completed != 0) _instrumentation?.AddOutboxPending(-completed);
    }

    internal ValueTask DispatchRecoveredAsync(OutboxDispatchBatch batch, CancellationToken cancellationToken) =>
        DispatchAsync(batch, cancellationToken, recovery: true);

}

internal static partial class DirectDispatchLog
{
    [LoggerMessage(2, LogLevel.Error, "Direct outbox dispatch failed. Durable rows remain available for recovery after claim expiry or failure recording.")]
    internal static partial void Failed(ILogger logger, Exception exception);
}
