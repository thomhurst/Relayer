using System.Data;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer;

internal sealed partial class SqlServerMaintenanceWorker : OutboxMaintenanceWorker
{
    private readonly TimeProvider _idleTimeProvider;
    private readonly string _connectionString;
    private readonly SqlServerMaintenanceOptions _options;
    private readonly SqlServerOutboxStore _store;
    private readonly SqlServerDirectDispatcher _dispatcher;
    private readonly ILogger<SqlServerMaintenanceWorker> _logger;
    private readonly Guid _owner = Guid.NewGuid();
    private readonly string _leases, _table, _dead, _inbox;
    private readonly string _acquire, _renew, _read, _highWater, _deleteDead, _deleteInbox;
    private readonly string _sweeperResource, _cleanupResource;
    private readonly OutboxRecord[] _rows;
    private readonly List<SqlServerBufferedMessage> _buffers = [];
    private readonly OutboxDispatchBatch _batch = new();
    private readonly byte[] _after = new byte[16], _through = new byte[16];
    private bool _hasAfter, _hasThrough;

    [RequiresUnreferencedCode("SqlClient runtime trimming is unsupported.")]
    [RequiresDynamicCode("SqlClient runtime Native AOT is unsupported.")]
    internal SqlServerMaintenanceWorker(string connectionString, SqlServerMaintenanceOptions options,
        IEnumerable<ITransport> transports, ILogger<SqlServerMaintenanceWorker> logger,
        ILogger<SqlServerDirectDispatcher>? dispatchLogger = null, RelayerInstrumentation? instrumentation = null, TimeProvider? idleTimeProvider = null)
        : base(2, options.BatchSize, options.CleanupBatchLimit,
            (options.DeadRetention is not null ? 1 : 0) | (options.InboxRetention is not null ? 2 : 0))
    {
        _connectionString = connectionString;
        _idleTimeProvider = idleTimeProvider ?? TimeProvider.System;
        _options = options;
        var settings = options.Validate();
        _store = new(connectionString, settings);
        _dispatcher = new(connectionString, settings, new() { BatchSize = options.BatchSize, QueueCapacity = 1,
            ClaimDuration = options.ClaimDuration }, transports, dispatchLogger ?? NullLogger<SqlServerDirectDispatcher>.Instance, _store, instrumentation);
        _logger = logger;
        _table = SqlServerOutboxSchema.Table(settings);
        _leases = SqlServerOutboxSchema.LeaseTable(settings);
        _dead = SqlServerOutboxSchema.DeadTable(settings);
        _inbox = SqlServerOutboxSchema.InboxTable(settings);
        _rows = new OutboxRecord[options.BatchSize];
        var resource = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(_leases)));
        _sweeperResource = resource + ":sweeper";
        _cleanupResource = resource + ":cleanup";
        _highWater = $"SELECT TOP (1) id FROM {_table} WHERE lane IS NULL AND status='pending' ORDER BY id DESC";
        _renew = $"UPDATE {_leases} SET expires_at=DATEADD(millisecond, @duration, SYSUTCDATETIME()) WHERE name=@name AND owner=@owner";
        _deleteDead = DeleteSql(_dead, "failed_at");
        _deleteInbox = DeleteSql(_inbox, "processed_at");
        _acquire = $"""
            DECLARE @result int;
            EXEC @result = sys.sp_getapplock @Resource=@resource, @LockMode='Exclusive',
                @LockOwner='Transaction', @LockTimeout=0;
            IF @result < 0 BEGIN SELECT CAST(0 AS bit); RETURN; END;
            UPDATE {_leases} WITH (UPDLOCK, HOLDLOCK) SET owner=@owner,
                expires_at=DATEADD(millisecond, @duration, SYSUTCDATETIME())
                WHERE name=@name AND (owner=@owner OR expires_at <= SYSUTCDATETIME());
            IF @@ROWCOUNT = 1 BEGIN SELECT CAST(1 AS bit); RETURN; END;
            IF EXISTS (SELECT 1 FROM {_leases} WHERE name=@name) BEGIN SELECT CAST(0 AS bit); RETURN; END;
            INSERT INTO {_leases} (name, owner, expires_at)
                VALUES (@name, @owner, DATEADD(millisecond, @duration, SYSUTCDATETIME()));
            SELECT CAST(1 AS bit);
            """;
        _read = $"""
            SELECT TOP (@limit) id, transport, message_type, destination, body, headers, ordering_key,
                deliver_at, created_at, lane, attempts, last_error, tenant_id
            FROM {_table} WHERE lane IS NULL AND status='pending'
                AND created_at <= DATEADD(millisecond, -@ageRemainder, DATEADD(day, -@ageDays, SYSUTCDATETIME()))
                AND (deliver_at IS NULL OR deliver_at <= SYSUTCDATETIME())
                AND (claim_expires_at IS NULL OR claim_expires_at <= SYSUTCDATETIME())
                AND (@after IS NULL OR id > @after) AND id <= @through ORDER BY id
            """;

    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var nextCleanup = DateTimeOffset.MinValue;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var scanned = 0;
                try
                {
                    scanned = await SweepAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error) { Failed(_logger, error); }
                // A failed recovery pass must not suppress the independently leased retention pass.
                if (DateTimeOffset.UtcNow >= nextCleanup)
                {
                    try
                    {
                        await CleanupAsync(stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                    catch (Exception error) { Failed(_logger, error); }
                    nextCleanup = DateTimeOffset.UtcNow + (CleanupPending ? TimeSpan.Zero : _options.CleanupInterval);
                }
                // Each turn releases database admission. Continue only within the fixed scan boundary;
                // its empty terminal pass provides the idle/retry delay even when every send failed.
                if (scanned == 0)
                    await Task.Delay(_options.SweepInterval, _idleTimeProvider, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            using var timeout = new CancellationTokenSource(_options.LeaseDuration);
            try { await ReleaseAsync(timeout.Token).ConfigureAwait(false); }
            catch (Exception error) { Failed(_logger, error); }
        }
    }

    internal ValueTask<int> SweepAsync(CancellationToken cancellationToken = default) => RunAsync(false, cancellationToken);
    protected override ValueTask<int> DeleteRetentionBatchAsync(int table, CancellationToken cancellationToken) =>
        RunAsync(true, cancellationToken, table);

    private async ValueTask<int> RunAsync(bool cleanup, CancellationToken cancellationToken, int cleanupTable = -1)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var name = cleanup ? "cleanup" : "sweeper";
        // Native transaction application locks avoid waiting behind an active connected pass.
        // The durable lease governs idle leadership and expires after an abrupt process exit.
        await using var acquire = new SqlCommand(_acquire, connection, transaction);
        acquire.Parameters.AddWithValue("@resource", cleanup ? _cleanupResource : _sweeperResource);
        acquire.Parameters.AddWithValue("@name", name);
        acquire.Parameters.AddWithValue("@owner", _owner);
        acquire.Parameters.AddWithValue("@duration", (int)_options.LeaseDuration.TotalMilliseconds);
        if (await acquire.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true) return cleanup ? -1 : 0;
        _store.UseDispatchTransaction(connection, transaction);
        var committed = false;
        try
        {
            var count = cleanup ? await DeleteExpiredAsync(connection, transaction, cleanupTable, cancellationToken).ConfigureAwait(false)
                : await DispatchAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
            await using var renew = new SqlCommand(_renew, connection, transaction);
            renew.Parameters.AddWithValue("@duration", (int)_options.LeaseDuration.TotalMilliseconds);
            renew.Parameters.AddWithValue("@name", name);
            renew.Parameters.AddWithValue("@owner", _owner);
            await renew.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            committed = true;
            return count;
        }
        catch
        {
            // A rolled-back traversal must revisit its rows.
            _hasAfter = _hasThrough = false;
            throw;
        }
        finally
        {
            _store.UseDispatchTransaction(null, null);
            _dispatcher.FinishRecovery(committed);
        }
    }

    private async ValueTask<int> DispatchAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken)
    {
        if (!_hasThrough)
        {
            await using var boundary = new SqlCommand(_highWater, connection, transaction);
            if (await boundary.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not byte[] through) return 0;
            through.CopyTo(_through, 0);
            _hasThrough = true;
        }
        await using var command = new SqlCommand(_read, connection, transaction);
        command.Parameters.AddWithValue("@limit", _rows.Length);
        AddDuration(command, "@ageDays", "@ageRemainder", _options.RecoveryDelay);
        command.Parameters.Add("@after", SqlDbType.Binary, 16).Value = _hasAfter ? _after : DBNull.Value;
        command.Parameters.Add("@through", SqlDbType.Binary, 16).Value = _through;
        var count = await _store.ReadRecoveryAsync(command, _rows, cancellationToken).ConfigureAwait(false);
        try
        {
            if (count == 0) { _hasAfter = _hasThrough = false; return 0; }
            _rows[count - 1].Id.TryWriteBytes(_after, bigEndian: true, out _);
            _hasAfter = true;
            var valid = 0;
            for (var i = 0; i < count; i++)
            {
                var row = _rows[i];
                if (row.TransportName.Length == 0)
                {
                    var claim = new OutboxClaim(row.Id, Guid.NewGuid());
                    var now = DateTimeOffset.UtcNow;
                    if (await _store.TryClaimAsync(claim, now, now + _options.ClaimDuration, cancellationToken).ConfigureAwait(false))
                    {
                        if (await _store.QuarantineAsync(claim, row.LastError!, cancellationToken).ConfigureAwait(false))
                            Quarantined(_logger, row.Id, row.LastError!);
                    }
                    continue;
                }
                if (valid == _buffers.Count) _buffers.Add(new());
                _buffers[valid++].Copy(row, 1, unordered: true);
            }
            _batch.Take(_buffers, valid, DateTimeOffset.UtcNow);
            await _dispatcher.DispatchRecoveredAsync(_batch, cancellationToken).ConfigureAwait(false);
            return count;
        }
        finally { _batch.Clear(); Array.Clear(_rows, 0, count); }
    }

    private async ValueTask<int> DeleteExpiredAsync(SqlConnection connection, SqlTransaction transaction, int table, CancellationToken cancellationToken)
    {
        return table switch
        {
            0 => await DeleteAsync(_deleteDead, _options.DeadRetention.GetValueOrDefault(), connection, transaction, cancellationToken).ConfigureAwait(false),
            _ => await DeleteAsync(_deleteInbox, _options.InboxRetention.GetValueOrDefault(), connection, transaction, cancellationToken).ConfigureAwait(false),
        };
    }

    private async ValueTask<int> DeleteAsync(string sql, TimeSpan retention, SqlConnection connection,
        SqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("@limit", _options.BatchSize);
        AddDuration(command, "@days", "@remainder", retention);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private string DeleteSql(string table, string column)
    {
        var preserve = column == "failed_at" ? $" AND NOT EXISTS (SELECT 1 FROM {_table} p WHERE p.id=d.id AND p.status='parked')" : "";
        return $";WITH expired AS (SELECT TOP (@limit) * FROM {table} d WHERE {column} <= DATEADD(millisecond, -@remainder, DATEADD(day, -@days, SYSUTCDATETIME())){preserve} ORDER BY {column}) DELETE FROM expired";
    }

    private static void AddDuration(SqlCommand command, string daysName, string remainderName, TimeSpan duration)
    {
        command.Parameters.AddWithValue(daysName, duration.Days);
        command.Parameters.AddWithValue(remainderName, (int)Math.Ceiling((duration - TimeSpan.FromDays(duration.Days)).TotalMilliseconds));
    }

    internal async ValueTask ReleaseAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand($"DELETE FROM {_leases} WHERE owner=@owner", connection);
        command.Parameters.AddWithValue("@owner", _owner);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public override void Dispose() { _dispatcher.Dispose(); base.Dispose(); }

    [LoggerMessage(2, LogLevel.Error, "SQL Server outbox row {MessageId} moved to dead storage: {Error}")]
    private static partial void Quarantined(ILogger logger, Guid messageId, string error);

    [LoggerMessage(1, LogLevel.Error, "SQL Server outbox maintenance failed. Durable rows remain available for the next pass.")]
    private static partial void Failed(ILogger logger, Exception exception);
}
