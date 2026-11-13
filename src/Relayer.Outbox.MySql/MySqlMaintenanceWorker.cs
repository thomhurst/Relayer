using MySqlConnector;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer.Transports;

namespace Relayer.Outbox.MySql;

internal sealed partial class MySqlMaintenanceWorker : BackgroundService
{
    private readonly MySqlDataSource _dataSource;
    private readonly MySqlMaintenanceOptions _options;
    private readonly MySqlOutboxStore _store;
    private readonly MySqlDirectDispatcher _dispatcher;
    private readonly ILogger<MySqlMaintenanceWorker> _logger;
    private readonly RelayerInstrumentation? _instrumentation;
    private readonly Guid _owner = Guid.NewGuid();
    private readonly string _leases, _table, _dead, _inbox;
    private readonly string _acquire, _renew, _read, _highWater, _deleteDead, _deleteInbox;
    private readonly string _sweeperResource, _cleanupResource;
    private readonly OutboxRecord[] _rows;
    private readonly List<BufferedOutboxMessage> _buffers = [];
    private readonly OutboxDispatchBatch _batch = new();
    private readonly byte[] _after = new byte[16], _through = new byte[16];
    private bool _hasAfter, _hasThrough;

    private int _cleanupStart;
    internal bool CleanupPending { get; private set; }

    internal MySqlMaintenanceWorker(MySqlDataSource dataSource, MySqlMaintenanceOptions options,
        IEnumerable<ITransport> transports, ILogger<MySqlMaintenanceWorker> logger,
        ILogger<MySqlDirectDispatcher>? dispatchLogger = null, RelayerInstrumentation? instrumentation = null)
    {
        _dataSource = dataSource;
        ValidateDataSource(dataSource);
        _options = options;
        var settings = options.Validate();
        _store = new(dataSource, settings);
        _dispatcher = new(dataSource, settings, new() { BatchSize = options.BatchSize, QueueCapacity = 1,
            ClaimDuration = options.ClaimDuration }, transports, dispatchLogger ?? NullLogger<MySqlDirectDispatcher>.Instance, _store, instrumentation);
        _logger = logger;
        _instrumentation = instrumentation;
        _table = MySqlOutboxSchema.Table(settings);
        _leases = MySqlOutboxSchema.LeaseTable(settings);
        _dead = MySqlOutboxSchema.DeadTable(settings);
        _inbox = MySqlOutboxSchema.InboxTable(settings);
        _rows = new OutboxRecord[options.BatchSize];
        var resource = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(new MySqlConnectionStringBuilder(dataSource.ConnectionString).Database + ":" + _leases)));
        _sweeperResource = resource[..48] + ":sweeper";
        _cleanupResource = resource[..48] + ":cleanup";
        _highWater = $"SELECT HEX(id) FROM {_table} WHERE lane IS NULL AND status='pending' ORDER BY id DESC LIMIT 1";
        _renew = $"UPDATE {_leases} SET expires_at=TIMESTAMPADD(MICROSECOND,@duration,UTC_TIMESTAMP(6)) WHERE name=@name AND owner=@owner";
        _deleteDead = DeleteSql(_dead, "failed_at");
        _deleteInbox = DeleteSql(_inbox, "processed_at");
        _acquire = $"""
            INSERT INTO {_leases} (name,owner,expires_at) VALUES (@name,@owner,TIMESTAMPADD(MICROSECOND,@duration,UTC_TIMESTAMP(6)))
            ON DUPLICATE KEY UPDATE
                owner=IF(owner=@owner OR expires_at<=UTC_TIMESTAMP(6),@owner,owner),
                expires_at=IF(owner=@owner,TIMESTAMPADD(MICROSECOND,@duration,UTC_TIMESTAMP(6)),expires_at);
            SELECT owner=@owner FROM {_leases} WHERE name=@name;
            """;
        _read = $"""
            SELECT id, transport, message_type, destination, body, headers, ordering_key,
                deliver_at, created_at, lane, attempts, last_error, tenant_id
            FROM {_table} WHERE lane IS NULL AND status='pending'
                AND created_at <= TIMESTAMPADD(MICROSECOND,-@age,UTC_TIMESTAMP(6))
                AND (deliver_at IS NULL OR deliver_at <= UTC_TIMESTAMP(6))
                AND (claim_expires_at IS NULL OR claim_expires_at <= UTC_TIMESTAMP(6))
                AND (@after IS NULL OR id > @after) AND id <= @through ORDER BY id LIMIT @limit
            """;
    }

    internal static void ValidateDataSource(MySqlDataSource dataSource)
    {
        var target = MySqlDirectDispatcher.ValidateDataSource(dataSource);
        if (target.Pooling && target.MaximumPoolSize < 2)
            throw new ArgumentException("MySQL maintenance requires MaximumPoolSize >= 2: one connection holds admission while another dispatches rows.", nameof(dataSource));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var nextCleanup = DateTimeOffset.MinValue;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await SweepAsync(stoppingToken).ConfigureAwait(false);
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
                await Task.Delay(_options.SweepInterval, stoppingToken).ConfigureAwait(false);
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
    internal async ValueTask<int> CleanupAsync(CancellationToken cancellationToken = default)
    {
        var count = 0;
        var active = 0;
        if (_options.DeadRetention is not null) active |= 1;
        if (_options.InboxRetention is not null) active |= 2;
        // Rotate before awaiting: a busy table or cancellation cannot monopolize subsequent turns.
        var first = _cleanupStart;
        _cleanupStart = (first + 1) % 2;
        // Only a successful turn may request an immediate continuation.
        CleanupPending = false;
        for (var round = 0; round < _options.CleanupBatchLimit && active != 0; round++)
        {
            for (var offset = 0; offset < 2; offset++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var table = (first + offset) % 2;
                var bit = 1 << table;
                if ((active & bit) == 0) continue;
                var deleted = await RunAsync(true, cancellationToken, table).ConfigureAwait(false);
                if (deleted < 0) { CleanupPending = false; return count; }
                count += deleted;
                if (deleted < _options.BatchSize) active &= ~bit;
            }
        }
        // A full final batch may have more behind it. Yield to recovery before admitting more.
        CleanupPending = active != 0;
        return count;
    }

    private async ValueTask<int> RunAsync(bool cleanup, CancellationToken cancellationToken, int cleanupTable = -1)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var name = cleanup ? "cleanup" : "sweeper";
        var resource = cleanup ? _cleanupResource : _sweeperResource;
        await using var admission = new MySqlCommand("SELECT GET_LOCK(@resource,0)", connection);
        admission.Parameters.AddWithValue("resource", resource);
        try
        {
            // Cancellation may arrive after MySQL acquired admission but before its reply arrives.
            // Always release, even when acquisition's outcome is unknown.
            if (Convert.ToInt32(await admission.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) != 1) return cleanup ? -1 : 0;
            await using var acquire = new MySqlCommand(_acquire, connection);
            acquire.Parameters.AddWithValue("name", name);
            acquire.Parameters.AddWithValue("owner", _owner.ToString("N"));
            acquire.Parameters.AddWithValue("duration", Micros(_options.LeaseDuration));
            if (Convert.ToInt32(await acquire.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) != 1) return cleanup ? -1 : 0;
            var count = cleanup ? await DeleteExpiredAsync(connection, cleanupTable, cancellationToken).ConfigureAwait(false)
                : await DispatchAsync(connection, cancellationToken).ConfigureAwait(false);
            await using var renew = new MySqlCommand(_renew, connection);
            renew.Parameters.AddWithValue("duration", Micros(_options.LeaseDuration));
            renew.Parameters.AddWithValue("name", name);
            renew.Parameters.AddWithValue("owner", _owner.ToString("N"));
            await renew.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return count;
        }
        catch
        {
            _hasAfter = _hasThrough = false;
            throw;
        }
        finally
        {
            // Named locks outlive transactions; explicitly release before returning a pooled session.
            // Cleanup must not replace a pass failure or inherit an unlimited command timeout.
            try
            {
                admission.CommandText = "SELECT RELEASE_LOCK(@resource)";
                admission.CommandTimeout = 3;
                // Use the native command timeout, whose cancellation/hard-timeout path breaks
                // an unresponsive session. A separate cancelled token could skip release entirely.
                await admission.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception error)
            {
                ReleaseFailed(_logger, error);
            }
        }
    }

    private async ValueTask<int> DispatchAsync(MySqlConnection connection, CancellationToken cancellationToken)
    {
        if (!_hasThrough)
        {
            await using var boundary = new MySqlCommand(_highWater, connection);
            if (await boundary.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not string through) return 0;
            Convert.FromHexString(through).CopyTo(_through, 0);
            _hasThrough = true;
        }
        await using var command = new MySqlCommand(_read, connection);
        command.Parameters.AddWithValue("@limit", _rows.Length);
        command.Parameters.AddWithValue("age", Micros(_options.RecoveryDelay));
        command.Parameters.Add("@after", MySqlDbType.Binary, 16).Value = _hasAfter ? _after : DBNull.Value;
        command.Parameters.Add("@through", MySqlDbType.Binary, 16).Value = _through;
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
                        var outcome = await _store.QuarantineAsync(claim, row.LastError!, cancellationToken).ConfigureAwait(false);
                        outcome.RecordTelemetry(_instrumentation);
                        if (outcome.Accepted) Quarantined(_logger, row.Id, row.LastError!);
                    }
                    continue;
                }
                if (valid == _buffers.Count) _buffers.Add(new());
                _buffers[valid++].Copy(row, 1);
            }
            _batch.Take(_buffers, valid, DateTimeOffset.UtcNow, recovered: true);
            await _dispatcher.DispatchRecoveredAsync(_batch, cancellationToken).ConfigureAwait(false);
            return count;
        }
        finally { _batch.Clear(); Array.Clear(_rows, 0, count); }
    }

    private async ValueTask<int> DeleteExpiredAsync(MySqlConnection connection, int table, CancellationToken cancellationToken)
    {
        return table switch
        {
            0 => await DeleteAsync(_deleteDead, _options.DeadRetention.GetValueOrDefault(), connection, cancellationToken).ConfigureAwait(false),
            _ => await DeleteAsync(_deleteInbox, _options.InboxRetention.GetValueOrDefault(), connection, cancellationToken).ConfigureAwait(false),
        };
    }

    private async ValueTask<int> DeleteAsync(string sql, TimeSpan retention, MySqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = new MySqlCommand(sql, connection);
        command.Parameters.AddWithValue("@limit", _options.BatchSize);
        command.Parameters.AddWithValue("age", Micros(retention));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private string DeleteSql(string table, string column)
    {
        var preserve = column == "failed_at" ? $" AND NOT EXISTS (SELECT 1 FROM {_table} p WHERE p.id={table}.id AND p.status='parked')" : "";
        return $"DELETE FROM {table} WHERE {column} <= TIMESTAMPADD(MICROSECOND,-@age,UTC_TIMESTAMP(6)){preserve} ORDER BY {column} LIMIT @limit";
    }

    private static long Micros(TimeSpan duration) => checked((long)Math.Ceiling(duration.TotalMicroseconds));

    internal async ValueTask ReleaseAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new MySqlCommand($"DELETE FROM {_leases} WHERE owner=@owner", connection);
        command.Parameters.AddWithValue("@owner", _owner.ToString("N"));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public override void Dispose() { _dispatcher.Dispose(); base.Dispose(); }

    [LoggerMessage(2, LogLevel.Error, "MySQL outbox row {MessageId} moved to dead storage: {Error}")]
    private static partial void Quarantined(ILogger logger, Guid messageId, string error);

    [LoggerMessage(3, LogLevel.Warning, "MySQL maintenance admission release failed. The original pass outcome is preserved.")]
    private static partial void ReleaseFailed(ILogger logger, Exception exception);

    [LoggerMessage(1, LogLevel.Error, "MySQL outbox maintenance failed. Durable rows remain available for the next pass.")]
    private static partial void Failed(ILogger logger, Exception exception);
}
