using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Outbox.MySql;

internal sealed class MySqlDirectDispatcher : OutboxDirectDispatchWorker<OutboxDispatchBatch>
{
    private readonly MySqlOutboxStore _store;
    private readonly MySqlDirectDispatchOptions _options;
    private readonly MySqlConnector.MySqlConnectionStringBuilder _target;
    private readonly string _prefix;

    internal MySqlDirectDispatcher(MySqlConnector.MySqlDataSource dataSource, MySqlOutboxOptions storeOptions,
        MySqlDirectDispatchOptions options, IEnumerable<ITransport> transports, ILogger<MySqlDirectDispatcher> logger,
        MySqlOutboxStore? store = null, RelayerInstrumentation? instrumentation = null)
        : base(options.QueueCapacity, options.BatchSize, options.ClaimDuration, transports, logger, instrumentation)
    {
        _store = store ?? new(dataSource, storeOptions);
        _target = ValidateDataSource(dataSource);
        _prefix = storeOptions.TablePrefix;
        _options = options;
    }

    internal static MySqlConnector.MySqlConnectionStringBuilder ValidateDataSource(MySqlConnector.MySqlDataSource source)
    {
        MySqlOutboxStore.ValidateDataSource(source);
        var target = new MySqlConnector.MySqlConnectionStringBuilder(source.ConnectionString);
        if (string.IsNullOrWhiteSpace(target.Database))
            throw new ArgumentException("MySQL direct dispatch requires an explicit Database in the data source connection string.", nameof(source));
        return target;
    }

    internal void ValidateStore(string prefix)
    {
        if (_prefix != prefix)
            throw new InvalidOperationException("The MySQL outbox and its direct dispatcher must use identical table prefixes.");
    }

    internal void ValidateConnection(MySqlConnector.MySqlConnection connection)
    {
        var writer = new MySqlConnector.MySqlConnectionStringBuilder(connection.ConnectionString);
        if (!StringComparer.OrdinalIgnoreCase.Equals(writer.Server, _target.Server) || writer.Port != _target.Port ||
            !StringComparer.Ordinal.Equals(connection.Database, _target.Database))
            throw new InvalidOperationException("The MySQL outbox transaction and its direct dispatcher must use the same server, port and database.");
    }

    // Called only after durable commit. Queue pressure and shutdown cannot make a committed transaction fail.
    internal void Committed(List<BufferedOutboxMessage> rows, int count)
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
        // Recovery records invalid destinations; notifications leave them for inspection.
        do
        {
            var id = batch.Rows[offset++].Id;
            if (!recovery) continue;
            var claim = new OutboxClaim(id, Guid.NewGuid());
            var now = DateTimeOffset.UtcNow;
            if (await _store.TryClaimAsync(claim, now, now + _options.ClaimDuration, cancellationToken).ConfigureAwait(false))
                (await _store.RecordDispatchFailureAsync(claim, error.Message, now, cancellationToken).ConfigureAwait(false)).RecordTelemetry(_instrumentation);
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
        _store.RecordRetryAsync(_claims[index], error.Message, DateTimeOffset.UtcNow, cancellationToken);

    internal ValueTask DispatchRecoveredAsync(OutboxDispatchBatch batch, CancellationToken cancellationToken) =>
        DispatchAsync(batch, cancellationToken, recovery: true);

    public override void Dispose()
    {
        base.Dispose();
        _store.Dispose();
    }
}

internal static partial class DirectDispatchLog
{
    [LoggerMessage(2, LogLevel.Error, "Direct outbox dispatch failed. Durable rows remain available for recovery after claim expiry or failure recording.")]
    internal static partial void Failed(ILogger logger, Exception exception);
}
