using System.Runtime.CompilerServices;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed partial class PostgreSqlMaintenanceWorker : BackgroundService
{
    private readonly NpgsqlDataSource _source;
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
    private readonly OutboxRecord[] _rows;
    private Guid? _after;
    private Guid? _through;

    internal PostgreSqlMaintenanceWorker(NpgsqlDataSource source, PostgreSqlMaintenanceOptions options,
        IEnumerable<ITransport> transports, ILogger<PostgreSqlMaintenanceWorker> logger,
        ILogger<PostgreSqlDirectDispatcher>? dispatchLogger = null, RelayerInstrumentation? instrumentation = null)
    {
        _source = source;
        _options = options;
        var store = options.Validate();
        _store = new(source, store);
        _logger = logger;
        _dispatcher = new(source, store, new() { BatchSize = options.BatchSize, QueueCapacity = 1, ClaimDuration = options.ClaimDuration },
            transports, dispatchLogger ?? NullLogger<PostgreSqlDirectDispatcher>.Instance, _store, instrumentation);
        _rows = new OutboxRecord[options.BatchSize];
        var schema = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + ".";
        string Table(string suffix) => schema + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + suffix);
        var leases = Table("leases");
        _acquire = $"INSERT INTO {leases} (name, owner, expires_at) VALUES (@name, @owner, clock_timestamp() + @duration) ON CONFLICT (name) DO UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at WHERE {leases}.owner = @owner OR {leases}.expires_at <= clock_timestamp() RETURNING true";
        _renew = $"UPDATE {leases} SET expires_at = clock_timestamp() + @duration WHERE name = @name AND owner = @owner";
        _release = $"DELETE FROM {leases} WHERE name IN ('sweeper', 'cleanup') AND owner = @owner";
        _highWaterMark = $"SELECT id FROM {Table("outbox")} WHERE lane IS NULL AND status = 'pending' ORDER BY id DESC LIMIT 1";
        _read = $"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id FROM {Table("outbox")} WHERE lane IS NULL AND status = 'pending' AND created_at <= clock_timestamp() - @age AND (deliver_at IS NULL OR deliver_at <= clock_timestamp()) AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp()) AND (@after IS NULL OR id > @after) AND id <= @through ORDER BY id LIMIT @limit";
        _dead = $"DELETE FROM {Table("outbox_dead")} WHERE id IN (SELECT id FROM {Table("outbox_dead")} WHERE failed_at <= clock_timestamp() - @retention ORDER BY failed_at, id LIMIT @limit)";
        _inbox = $"DELETE FROM {Table("inbox")} WHERE (consumer, message_id) IN (SELECT consumer, message_id FROM {Table("inbox")} WHERE processed_at <= clock_timestamp() - @retention ORDER BY processed_at, consumer, message_id LIMIT @limit)";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var nextCleanup = DateTimeOffset.MinValue;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try { await SweepAsync(stoppingToken).ConfigureAwait(false); }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error) { Failed(_logger, "sweeper", error); }
                if (DateTimeOffset.UtcNow >= nextCleanup)
                {
                    try { await CleanupAsync(stoppingToken).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                    catch (Exception error) { Failed(_logger, "cleanup", error); }
                    nextCleanup = DateTimeOffset.UtcNow + _options.CleanupInterval;
                }
                await Task.Delay(_options.SweepInterval, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            using var timeout = new CancellationTokenSource(_options.LeaseDuration);
            try { await ReleaseAsync(timeout.Token).ConfigureAwait(false); }
            catch (Exception error) { Failed(_logger, "lease release", error); }
        }
    }

    internal ValueTask<int> SweepAsync(CancellationToken cancellationToken) => RunAsync("sweeper", false, cancellationToken);
    internal ValueTask<int> CleanupAsync(CancellationToken cancellationToken) => RunAsync("cleanup", true, cancellationToken);

    // The native replication slot serializes CDC readers. Fenced row claims still arbitrate with
    // direct dispatch and the recovery leader; CDC must not wait for the sweeper's idle lease.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DispatchCommittedAsync(CancellationToken cancellationToken)
    {
        _after = null;
        _through = null;
        do
        {
            await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await DispatchAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        } while (_through is not null);
    }

    private async ValueTask<int> RunAsync(string name, bool cleanup, CancellationToken cancellationToken)
    {
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        // Keep this row locked until the native sends or retention deletes finish. Connected owners never overlap.
        await using var lease = new NpgsqlCommand(_acquire, connection, transaction);
        lease.Parameters.AddWithValue("name", name);
        lease.Parameters.AddWithValue("owner", _owner);
        lease.Parameters.AddWithValue("duration", _options.LeaseDuration);
        if (await lease.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true) return 0;
        var count = cleanup
            ? await DeleteExpiredAsync(connection, transaction, cancellationToken).ConfigureAwait(false)
            : await DispatchAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        // Start the idle lease at completion, not at the start of a potentially slow broker call.
        await using var renew = new NpgsqlCommand(_renew, connection, transaction);
        renew.Parameters.AddWithValue("name", name);
        renew.Parameters.AddWithValue("owner", _owner);
        renew.Parameters.AddWithValue("duration", _options.LeaseDuration);
        await renew.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return count;
    }

    private async ValueTask<int> DispatchAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        if (_through is null)
        {
            await using var boundary = new NpgsqlCommand(_highWaterMark, connection, transaction);
            if (await boundary.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not Guid through) return 0;
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
            if (count == 0) _through = null;
            _store.UseDispatchTransaction(connection, transaction);
            await _dispatcher.DispatchRecoveredAsync(_rows.AsMemory(0, count), cancellationToken, propagateDatabaseFailures: true).ConfigureAwait(false);
            return count;
        }
        finally
        {
            _store.UseDispatchTransaction(null, null);
            Array.Clear(_rows, 0, count);
        }
    }

    private async ValueTask<int> DeleteExpiredAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        var count = 0;
        if (_options.DeadRetention is { } dead)
            count += await DeleteAsync(connection, transaction, _dead, dead, cancellationToken).ConfigureAwait(false);
        if (_options.InboxRetention is { } inbox)
            count += await DeleteAsync(connection, transaction, _inbox, inbox, cancellationToken).ConfigureAwait(false);
        return count;
    }

    private async ValueTask<int> DeleteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string sql, TimeSpan retention, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("retention", retention);
        command.Parameters.AddWithValue("limit", _options.BatchSize);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "PostgreSQL outbox {Job} failed. Durable work remains available for a later pass.")]
    private static partial void Failed(ILogger logger, string job, Exception error);
}
