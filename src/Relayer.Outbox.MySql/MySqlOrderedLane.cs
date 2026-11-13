using System.Runtime.CompilerServices;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

internal sealed class MySqlOrderedLane : IDisposable
{
    private readonly MySqlDataSource _source;
    private readonly MySqlOutboxStore _reader;
    private readonly MySqlStoreParameters _read;
    private readonly MySqlStoreParameters _ready;
    private readonly MySqlStoreParameters _claim;
    private readonly MySqlStoreParameters _complete;
    private readonly MySqlStoreParameters _fail;
    private readonly MySqlStoreParameters _retry;

    internal MySqlOrderedLane(MySqlDataSource source, MySqlOutboxOptions options, TimeSpan duration, int maxAttempts)
    {
        _source = source;
        _reader = new(source, options);
        var table = MySqlOutboxSchema.Table(options);
        var heads = $"FROM {table} AS pending WHERE lane = CAST(@lane AS SIGNED) AND status = 'pending' AND NOT EXISTS (SELECT 1 FROM {table} AS parked WHERE parked.lane = pending.lane AND parked.status = 'parked' AND BINARY parked.ordering_key = BINARY pending.ordering_key AND parked.seq < pending.seq) ORDER BY seq LIMIT @limit";
        _read = new($"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id, seq {heads}");
        _ready = new($"SELECT (deliver_at IS NULL OR deliver_at <= UTC_TIMESTAMP(6)) AND (claim_expires_at IS NULL OR claim_expires_at <= UTC_TIMESTAMP(6)) {heads}");
        const string fenced = "id = @id AND claim_token = @token AND claim_expires_at > UTC_TIMESTAMP(6)";
        _claim = new($"UPDATE {table} SET claim_token = @token, claim_expires_at = TIMESTAMPADD(MICROSECOND, @duration, UTC_TIMESTAMP(6)) WHERE id = @id AND status = 'pending' AND (deliver_at IS NULL OR deliver_at <= UTC_TIMESTAMP(6)) AND (claim_expires_at IS NULL OR claim_expires_at <= UTC_TIMESTAMP(6))");
        _claim.Command.Parameters.AddWithValue("duration", duration.Ticks / 10);
        _complete = new($"DELETE FROM {table} WHERE {fenced}");
        _retry = new(MySqlRetrySql.Update(table, fenced, "UTC_TIMESTAMP(6)"));
        _fail = new($"UPDATE {table} SET status = IF(attempts + 1 >= @maximum, 'parked', 'pending'), attempts = attempts + 1, last_error = @error, claim_token = NULL, claim_expires_at = NULL WHERE {fenced}");
        _fail.Command.Parameters.AddWithValue("maximum", maxAttempts);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask FindReadyAsync(bool[] assigned, bool[] ready, CancellationToken cancellationToken)
    {
        Array.Clear(ready);
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var command = _ready.Command;
        command.Connection = connection;
        try
        {
            for (var lane = 0; lane < assigned.Length; lane++)
            {
                if (!assigned[lane]) continue;
                _ready.Read(lane, null, 1);
                ready[lane] = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture) == 1;
            }
        }
        finally { command.Connection = null; }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadAsync(MySqlConnection connection, MySqlTransaction transaction,
        int lane, Memory<OutboxRecord> rows, CancellationToken cancellationToken)
    {
        var command = _read.Command;
        _read.Read(lane, null, rows.Length);
        command.Connection = connection;
        command.Transaction = transaction;
        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return await _reader.ReadRowsAsync(reader, rows, cancellationToken, includeOrderedSequence: true, isolateMalformed: true).ConfigureAwait(false);
        }
        finally { command.Transaction = null; command.Connection = null; }
    }

    internal ValueTask<bool> ClaimAsync(MySqlConnection connection, MySqlTransaction transaction, OutboxClaim claim,
        CancellationToken cancellationToken) => MutateAsync(connection, transaction, _claim, claim, null, cancellationToken);
    internal ValueTask<bool> CompleteAsync(MySqlConnection connection, MySqlTransaction transaction, OutboxClaim claim,
        CancellationToken cancellationToken) => MutateAsync(connection, transaction, _complete, claim, null, cancellationToken);
    internal ValueTask<bool> FailAsync(MySqlConnection connection, MySqlTransaction transaction, OutboxClaim claim,
        string error, CancellationToken cancellationToken) => MutateAsync(connection, transaction, _fail, claim, error, cancellationToken);

    internal ValueTask<bool> RetryAsync(MySqlConnection connection, MySqlTransaction transaction, OutboxClaim claim,
        string error, CancellationToken cancellationToken) => MutateAsync(connection, transaction, _retry, claim, error, cancellationToken);

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<bool> MutateAsync(MySqlConnection connection, MySqlTransaction transaction,
        MySqlStoreParameters operation, OutboxClaim claim, string? error, CancellationToken cancellationToken)
    {
        var command = operation.Command;
        operation.Claim(claim);
        command.Connection = connection;
        command.Transaction = transaction;
        command.Parameters["error"].Value = (object?)error ?? DBNull.Value;
        try { return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1; }
        finally { command.Transaction = null; command.Connection = null; command.Parameters["error"].Value = DBNull.Value; }
    }

    public void Dispose()
    {
        _reader.Dispose(); _read.Command.Dispose(); _ready.Command.Dispose();
        _claim.Command.Dispose(); _complete.Command.Dispose(); _fail.Command.Dispose(); _retry.Command.Dispose();
    }
}
