using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed partial class PostgreSqlMaintenanceWorker : OutboxMaintenanceWorker
{
    private readonly NpgsqlDataSource _source;
    private readonly TimeProvider? _admissionTimeProvider;
    private readonly PostgreSqlMaintenanceOptions _options;
    private readonly PostgreSqlOutboxStore _store;
    private readonly PostgreSqlDirectDispatcher _dispatcher;
    private readonly ILogger<PostgreSqlMaintenanceWorker> _logger;
    private readonly string _owner = Guid.NewGuid().ToString("N");
    private readonly string _acquire;
    private readonly string _renew;
    private readonly string _release;
    private readonly string _highWaterMark;
    private readonly string _read;
    private readonly string _dead;
    private readonly string _inbox;
    private readonly string _sent;
    private readonly string _depth;
    private readonly OutboxRecord[] _rows;
    // At-least-once local rows from the last pass; they run after it commits and releases its connection.
    private readonly OutboxRecord[] _detached;
    private int _detachedCount;
    private Guid? _after;
    private int _localStop;
    private readonly CancellationTokenSource _idleStop = new();
    private Guid? _through;

    internal PostgreSqlMaintenanceWorker(NpgsqlDataSource source, PostgreSqlMaintenanceOptions options,
        IEnumerable<ITransport> transports, ILogger<PostgreSqlMaintenanceWorker> logger,
        ILogger<PostgreSqlDirectDispatcher>? dispatchLogger = null, RelayerInstrumentation? instrumentation = null, TimeProvider? admissionTimeProvider = null, IServiceScopeFactory? scopes = null, TimeProvider? idleTimeProvider = null, string? storeName = null)
        : base(3, options.BatchSize, options.CleanupBatchLimit,
            (options.RetainSentMessages ? 1 : 0) | (options.DeadRetention is not null ? 2 : 0) | (options.InboxRetention is not null ? 4 : 0),
            options.SweepInterval, options.CleanupInterval, options.LeaseDuration, idleTimeProvider,
            storeName is null ? null : instrumentation?.TrackOutboxDepth("postgresql", storeName))
    {
        _source = source;
        _admissionTimeProvider = admissionTimeProvider;
        _options = options;
        var store = options.Validate();
        _store = new(source, store);
        _logger = logger;
        _dispatcher = new(source, store, new()
        {
            BatchSize = options.BatchSize,
            QueueCapacity = 1,
            ClaimDuration = options.ClaimDuration
        },
            transports, dispatchLogger ?? NullLogger<PostgreSqlDirectDispatcher>.Instance, _store, instrumentation, scopes);
        _rows = new OutboxRecord[options.BatchSize];
        _detached = new OutboxRecord[options.BatchSize];
        var schema = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + ".";

        string Table(string suffix)
        {
            return schema + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + suffix);
        }

        var leases = Table("leases");
        _acquire = $"INSERT INTO {leases} (name, owner, expires_at) VALUES (@name, @owner, clock_timestamp() + @duration) ON CONFLICT (name) DO UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at WHERE {leases}.owner = @owner OR {leases}.expires_at <= clock_timestamp() RETURNING true";
        _renew = $"UPDATE {leases} SET expires_at = clock_timestamp() + @duration WHERE name = @name AND owner = @owner";
        _release = $"DELETE FROM {leases} WHERE name IN ('sweeper', 'cleanup') AND owner = @owner";
        _highWaterMark = $"SELECT id FROM {Table("outbox")} WHERE lane IS NULL AND status = 'pending' ORDER BY id DESC LIMIT 1";
        _read = $"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id, seq FROM {Table("outbox")} WHERE lane IS NULL AND status = 'pending' AND created_at <= clock_timestamp() - @age AND (deliver_at IS NULL OR deliver_at <= clock_timestamp()) AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp()) AND (@after IS NULL OR id > @after) AND id <= @through ORDER BY id LIMIT @limit";
        _dead = $"DELETE FROM {Table("outbox_dead")} WHERE id IN (SELECT id FROM {Table("outbox_dead")} WHERE failed_at <= clock_timestamp() - @retention AND NOT EXISTS (SELECT 1 FROM {Table("outbox")} AS parked WHERE parked.id = {Table("outbox_dead")}.id AND parked.status = 'parked') ORDER BY failed_at, id LIMIT @limit)";
        _depth = $"SELECT (SELECT count(*) FROM {Table("outbox_dead")}), (SELECT count(*) FROM {Table("outbox")} WHERE status = 'parked')";
        // Recheck age on the target row after a concurrent completion refreshes it under a row lock.
        _sent = $"DELETE FROM {Table("outbox_sent")} WHERE sent_at <= clock_timestamp() - @retention AND id IN (SELECT id FROM {Table("outbox_sent")} WHERE sent_at <= clock_timestamp() - @retention ORDER BY sent_at, id LIMIT @limit)";
        _inbox = $"DELETE FROM {Table("inbox")} WHERE (consumer, message_id) IN (SELECT consumer, message_id FROM {Table("inbox")} WHERE processed_at <= clock_timestamp() - @retention ORDER BY processed_at, consumer, message_id LIMIT @limit)";
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await PostgreSqlSentSchema.ValidateAsync(_source, _options.RetainSentMessages, _options.Schema,
            _options.TablePrefix, cancellationToken).ConfigureAwait(false);
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override bool MaintenanceStopping => Volatile.Read(ref _localStop) != 0;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var idleStop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _idleStop.Token);
        await RunMaintenanceAsync(stoppingToken, idleStop.Token).ConfigureAwait(false);
    }

    protected override ValueTask<int> RecoverAsync(CancellationToken cancellationToken)
    {
        return SweepAsync(cancellationToken);
    }

    protected override ValueTask ReleaseMaintenanceAsync(CancellationToken cancellationToken)
    {
        return ReleaseAsync(cancellationToken);
    }

    protected override void ReportFailure(string job, Exception error)
    {
        Failed(_logger, job, error);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_dispatcher.HasLocalTransport)
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        Interlocked.Exchange(ref _localStop, 1);
        _dispatcher.Quiesce();
        // Wake idle maintenance without cancelling the token draining active local deliveries.
        await _idleStop.CancelAsync().ConfigureAwait(false);

        try
        {
            if (ExecuteTask is { } task)
            {
                await task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal ValueTask<int> SweepAsync(CancellationToken cancellationToken)
    {
        return RunAsync("sweeper", false, cancellationToken);
    }

    internal ValueTask<int> SweepAsync(TimeSpan turnBudget, CancellationToken cancellationToken)
    {
        return RunAsync("sweeper", false, cancellationToken, turnBudget);
    }

    protected override ValueTask<int> DeleteRetentionBatchAsync(int table, CancellationToken cancellationToken)
    {
        return RunAsync("cleanup", true, cancellationToken, cleanupTable: table);
    }

    // The native replication slot serializes CDC readers. Fenced row claims still arbitrate with
    // direct dispatch and the recovery leader; CDC must not wait for the sweeper's idle lease.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DispatchCommittedAsync(CancellationToken cancellationToken)
    {
        _after = null;
        _through = null;

        do
        {
            try
            {
                await DispatchCommittedPassAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                ClearDetached();
                throw;
            }

            await DispatchDetachedAsync(cancellationToken).ConfigureAwait(false);
        } while (_through is not null);
    }

    private async ValueTask DispatchCommittedPassAsync(CancellationToken cancellationToken)
    {
        var committed = false;

        try
        {
            await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await DispatchAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            committed = true;
        }
        finally
        {
            _dispatcher.FinishRecovery(committed);
        }
    }

    private async ValueTask<int> RunAsync(string name, bool cleanup, CancellationToken cancellationToken, TimeSpan? turnBudget = null, int cleanupTable = -1)
    {
        var started = turnBudget.HasValue ? Stopwatch.GetTimestamp() : 0;
        int count;
        // Rows deferred by a pass that did not commit are revisited by a later pass instead.
        try
        {
            count = await RunPassAsync(name, cleanup, cancellationToken, started, turnBudget, cleanupTable).ConfigureAwait(false);
        }
        catch
        {
            ClearDetached();
            throw;
        }

        if (!cleanup)
        {
            await DispatchDetachedAsync(cancellationToken, turnBudget.HasValue ? (turnBudget.Value - Stopwatch.GetElapsedTime(started)) / 2 : null).ConfigureAwait(false);
        }

        return count;
    }

    private async ValueTask<int> RunPassAsync(string name, bool cleanup, CancellationToken cancellationToken, long started, TimeSpan? turnBudget, int cleanupTable)
    {
        var committed = false;

        try
        {
            await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            // Keep this row locked until the native sends or retention deletes finish. Connected owners never overlap.
            await using var lease = new NpgsqlCommand(_acquire, connection, transaction);
            lease.Parameters.AddWithValue("name", name);
            lease.Parameters.AddWithValue("owner", _owner);
            lease.Parameters.AddWithValue("duration", _options.LeaseDuration);

            if (await lease.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true)
            {
                return cleanup ? -1 : 0;
            }

            var count = cleanup
                ? await DeleteExpiredAsync(connection, transaction, cleanupTable, cancellationToken).ConfigureAwait(false)
                : await DispatchAsync(connection, transaction, cancellationToken, turnBudget - (turnBudget.HasValue ? Stopwatch.GetElapsedTime(started) : TimeSpan.Zero)).ConfigureAwait(false);
            // Start the idle lease at completion, not at the start of a potentially slow broker call.
            await using var renew = new NpgsqlCommand(_renew, connection, transaction);
            renew.Parameters.AddWithValue("name", name);
            renew.Parameters.AddWithValue("owner", _owner);
            renew.Parameters.AddWithValue("duration", _options.LeaseDuration);
            await renew.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            committed = true;
            return count;
        }
        finally
        {
            if (!cleanup)
            {
                _dispatcher.FinishRecovery(committed);
            }
        }
    }

    private async ValueTask<int> DispatchAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken, TimeSpan? turnBudget = null)
    {
        var started = turnBudget.HasValue ? Stopwatch.GetTimestamp() : 0;

        if (_through is null)
        {
            await using var boundary = new NpgsqlCommand(_highWaterMark, connection, transaction);

            if (await boundary.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not Guid through)
            {
                return 0;
            }

            _through = through;
        }

        await using var command = new NpgsqlCommand(_read, connection, transaction);
        command.Parameters.AddWithValue("age", _options.RecoveryDelay);
        command.Parameters.Add(new NpgsqlParameter<Guid?>("after", NpgsqlDbType.Uuid) { TypedValue = _after });
        command.Parameters.Add(new NpgsqlParameter<Guid?>("through", NpgsqlDbType.Uuid) { TypedValue = _through });
        command.Parameters.AddWithValue("limit", _rows.Length);
        var count = await _store.ReadRowsAsync(command, _rows, cancellationToken).ConfigureAwait(false);

        try
        {
            _after = count == 0 ? null : _rows[count - 1].Id;
            // A fixed boundary forces wrapping even while newer rows arrive, revisiting failed or newly eligible rows.
            if (count == 0)
            {
                _through = null;
            }
            // An at-least-once claim must commit before its handler runs, and the pass must not hold its
            // connection or transaction for that run; defer those rows until the pass commits.
            var enlisted = 0;

            for (var i = 0; i < count; i++)
            {
                if (_dispatcher.IsAtLeastOnce(_rows[i]))
                {
                    _detached[_detachedCount++] = _rows[i];
                }
                else
                {
                    _rows[enlisted++] = _rows[i];
                }
            }

            _store.UseDispatchTransaction(connection, transaction);
            // Preserve native destination batching; stop before another group and reserve time to commit.
            // The UUID boundary still wraps, so unadmitted durable rows are revisited.
            await _dispatcher.DispatchRecoveredAsync(_rows.AsMemory(0, enlisted), cancellationToken, propagateDatabaseFailures: true, admissionBudget: turnBudget.HasValue
                ? (turnBudget.Value - Stopwatch.GetElapsedTime(started)) / 2 : null, admissionTimeProvider: _admissionTimeProvider).ConfigureAwait(false);
            return count;
        }
        finally
        {
            _store.UseDispatchTransaction(null, null);
            Array.Clear(_rows, 0, count);
        }
    }

    // The rows' own fenced claims arbitrate with direct dispatch, as they do outside recovery. Database
    // failures still reach the caller, so CDC never checkpoints past them; each claim and failure
    // transition commits on its own, so poison recorded before a failure is reported either way.
    private async ValueTask DispatchDetachedAsync(CancellationToken cancellationToken, TimeSpan? admissionBudget = null)
    {
        if (_detachedCount == 0)
        {
            return;
        }

        try
        {
            await _dispatcher.DispatchRecoveredAsync(_detached.AsMemory(0, _detachedCount), cancellationToken,
                propagateDatabaseFailures: true, admissionBudget: admissionBudget, admissionTimeProvider: _admissionTimeProvider).ConfigureAwait(false);
        }
        finally
        {
            _dispatcher.FinishRecovery(committed: true);
            ClearDetached();
        }
    }

    private void ClearDetached()
    {
        Array.Clear(_detached, 0, _detachedCount);
        _detachedCount = 0;
    }

    private async ValueTask<int> DeleteExpiredAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int table, CancellationToken cancellationToken)
    {
        return table switch
        {
            0 => await DeleteAsync(connection, transaction, _sent, _options.SentRetention, cancellationToken).ConfigureAwait(false),
            1 => await DeleteAsync(connection, transaction, _dead, _options.DeadRetention.GetValueOrDefault(), cancellationToken).ConfigureAwait(false),
            _ => await DeleteAsync(connection, transaction, _inbox, _options.InboxRetention.GetValueOrDefault(), cancellationToken).ConfigureAwait(false),
        };
    }

    private async ValueTask<int> DeleteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string sql, TimeSpan retention, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("retention", retention);
        command.Parameters.AddWithValue("limit", _options.BatchSize);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async ValueTask<(long Dead, long Parked)> CountDepthAsync(CancellationToken cancellationToken)
    {
        await using var command = _source.CreateCommand(_depth);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    internal async ValueTask ReleaseAsync(CancellationToken cancellationToken)
    {
        await using var command = _source.CreateCommand(_release);
        command.Parameters.AddWithValue("owner", _owner);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public override void Dispose()
    {
        base.Dispose();
        _dispatcher.Dispose();
        _idleStop.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "PostgreSQL outbox {Job} failed. Durable work remains available for a later pass.")]
    private static partial void Failed(ILogger logger, string job, Exception error);
}
