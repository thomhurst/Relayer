using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;

namespace Relayer.Outbox.PostgreSql;

internal sealed class PostgreSqlOrderedLane
{
    private readonly PostgreSqlOutboxStore _reader;
    private readonly string _read;
    private readonly string _claim;
    private readonly string _complete;
    private readonly string _fail;
    private readonly TimeSpan _duration;

    internal PostgreSqlOrderedLane(NpgsqlDataSource source, PostgreSqlOutboxOptions options, TimeSpan duration)
    {
        _reader = new(source, options);
        _duration = duration;
        var table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        // Read scheduled and claimed heads too. Parked keys are excluded without parking the rest of the lane.
        _read = $"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id FROM {table} AS pending WHERE lane = @lane AND status = 'pending' AND NOT EXISTS (SELECT 1 FROM {table} AS parked WHERE parked.lane = @lane AND parked.status = 'parked' AND parked.ordering_key = pending.ordering_key AND parked.id < pending.id) ORDER BY id LIMIT @limit";
        var fenced = "id = @id AND claim_token = @token AND claim_expires_at > clock_timestamp()";
        _claim = $"UPDATE {table} SET claim_token = @token, claim_expires_at = clock_timestamp() + @duration WHERE id = @id AND status = 'pending' AND (deliver_at IS NULL OR deliver_at <= clock_timestamp()) AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp()) RETURNING true";
        _complete = $"DELETE FROM {table} WHERE {fenced} RETURNING true";
        _fail = $"UPDATE {table} SET attempts = attempts + 1, last_error = @error, claim_token = NULL, claim_expires_at = NULL WHERE {fenced} RETURNING true";
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        int lane, Memory<OutboxRecord> rows, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(_read, connection, transaction);
        command.Parameters.AddWithValue("lane", lane);
        command.Parameters.AddWithValue("limit", rows.Length);
        return await _reader.ReadRowsAsync(command, rows, cancellationToken).ConfigureAwait(false);
    }

    internal ValueTask<bool> ClaimAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        OutboxClaim claim, CancellationToken cancellationToken) => MutateAsync(connection, transaction, _claim, claim, null, cancellationToken);

    internal ValueTask<bool> CompleteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        OutboxClaim claim, CancellationToken cancellationToken) => MutateAsync(connection, transaction, _complete, claim, null, cancellationToken);

    internal ValueTask<bool> FailAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        OutboxClaim claim, string error, CancellationToken cancellationToken) => MutateAsync(connection, transaction, _fail, claim, error, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> MutateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql,
        OutboxClaim claim, string? error, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid) { TypedValue = claim.MessageId });
        command.Parameters.Add(new NpgsqlParameter<Guid>("token", NpgsqlDbType.Uuid) { TypedValue = claim.Token });
        if (sql == _claim) command.Parameters.Add(new NpgsqlParameter<TimeSpan>("duration", NpgsqlDbType.Interval) { TypedValue = _duration });
        if (error is not null) command.Parameters.AddWithValue("error", error);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }
}
