using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;

namespace Relayer.Outbox.PostgreSql;

internal sealed class PostgreSqlOrderedLane : IDisposable
{
    private readonly NpgsqlDataSource _source;
    private readonly PostgreSqlOutboxStore _reader;
    private readonly string _read;
    private readonly string _ready;
    private readonly List<int> _assigned = [];
    private readonly NpgsqlCommand _claim;
    private readonly NpgsqlCommand _complete;
    private readonly NpgsqlCommand _fail;
    private readonly NpgsqlCommand _failImmediately;

    internal PostgreSqlOrderedLane(NpgsqlDataSource source, PostgreSqlOutboxOptions options, TimeSpan duration)
    {
        _source = source;
        _reader = new(source, options);
        var table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        // Read scheduled and claimed heads too. Parked keys are excluded without parking the rest of the lane.
        _read = $"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id, seq FROM {table} AS pending WHERE lane = @lane AND status = 'pending' AND NOT EXISTS (SELECT 1 FROM {table} AS parked WHERE parked.lane = @lane AND parked.status = 'parked' AND parked.ordering_key = pending.ordering_key AND parked.seq < pending.seq) ORDER BY seq LIMIT @limit";
        // Probe only each assigned lane's first non-parked head. Re-read under the lease lock before sending.
        _ready = $"SELECT requested.lane FROM unnest(@lanes) AS requested(lane) CROSS JOIN LATERAL (SELECT deliver_at, claim_expires_at FROM {table} AS pending WHERE pending.lane = requested.lane AND status = 'pending' AND NOT EXISTS (SELECT 1 FROM {table} AS parked WHERE parked.lane = requested.lane AND parked.status = 'parked' AND parked.ordering_key = pending.ordering_key AND parked.seq < pending.seq) ORDER BY seq LIMIT 1) AS head WHERE (head.deliver_at IS NULL OR head.deliver_at <= clock_timestamp()) AND (head.claim_expires_at IS NULL OR head.claim_expires_at <= clock_timestamp())";
        var fenced = "id = @id AND claim_token = @token AND claim_expires_at > clock_timestamp()";
        _claim = CreateMutation($"UPDATE {table} SET claim_token = @token, claim_expires_at = clock_timestamp() + @duration WHERE id = @id AND status = 'pending' AND (deliver_at IS NULL OR deliver_at <= clock_timestamp()) AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())");
        _claim.Parameters.Add(new NpgsqlParameter<TimeSpan>("duration", NpgsqlDbType.Interval) { TypedValue = duration });
        var complete = $"DELETE FROM {table} WHERE {fenced}";
        if (options.RetainSentMessages)
        {
            var sent = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
                PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox_sent");
            // INSERT's affected-row count preserves the existing ExecuteNonQuery completion contract.
            complete = $"WITH deleted AS ({complete} RETURNING id, destination) INSERT INTO {sent} (id, destination, sent_at) SELECT id, destination, clock_timestamp() FROM deleted ON CONFLICT (id) DO UPDATE SET destination = EXCLUDED.destination, sent_at = EXCLUDED.sent_at";
        }
        _complete = CreateMutation(complete);
        var dead = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox_dead");
        _fail = CreateMutation(PostgreSqlPoisonSql.Failure(table, dead, fenced, options.MaxDispatchAttempts));
        _fail.Parameters.Add(new NpgsqlParameter<string>("error", NpgsqlDbType.Text));
        _failImmediately = CreateMutation(PostgreSqlPoisonSql.Failure(table, dead, fenced, 1));
        _failImmediately.Parameters.Add(new NpgsqlParameter<string>("error", NpgsqlDbType.Text));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask FindReadyAsync(bool[] assigned, bool[] ready, CancellationToken cancellationToken)
    {
        Array.Clear(ready);
        _assigned.Clear();
        for (var lane = 0; lane < assigned.Length; lane++)
            if (assigned[lane]) _assigned.Add(lane);
        if (_assigned.Count == 0) return;
        using var command = _source.CreateCommand(_ready);
        command.Parameters.Add(new NpgsqlParameter<List<int>>("lanes", NpgsqlDbType.Array | NpgsqlDbType.Integer) { TypedValue = _assigned });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false)) ready[reader.GetInt32(0)] = true;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        int lane, Memory<OutboxRecord> rows, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(_read, connection, transaction);
        command.Parameters.AddWithValue("lane", lane);
        command.Parameters.AddWithValue("limit", rows.Length);
        return await _reader.ReadRowsAsync(command, rows, cancellationToken, includeOrderedSequence: true).ConfigureAwait(false);
    }

    internal ValueTask<bool> ClaimAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        OutboxClaim claim, CancellationToken cancellationToken) => MutateAsync(connection, transaction, _claim, claim, null, cancellationToken);

    internal ValueTask<bool> CompleteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        OutboxClaim claim, CancellationToken cancellationToken) => MutateAsync(connection, transaction, _complete, claim, null, cancellationToken);

    internal ValueTask<bool> FailAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        OutboxClaim claim, string error, CancellationToken cancellationToken, bool permanent = false) => MutateAsync(connection, transaction, permanent ? _failImmediately : _fail, claim, error, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> MutateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, NpgsqlCommand command,
        OutboxClaim claim, string? error, CancellationToken cancellationToken)
    {
        command.Connection = connection;
        command.Transaction = transaction;
        SetClaim(command, claim);
        if (error is not null) ((NpgsqlParameter<string>)command.Parameters[2]).TypedValue = error;
        try
        {
            if (error is not null) return (int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))! != 0;
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
        }
        finally
        {
            command.Transaction = null;
            command.Connection = null;
            if (error is not null) ((NpgsqlParameter<string>)command.Parameters[2]).TypedValue = null;
        }
    }

    // Commands belong to the single dispatch loop. Typed parameters avoid boxing claim values on every row.
    internal static NpgsqlCommand SetClaim(NpgsqlCommand command, OutboxClaim claim)
    {
        ((NpgsqlParameter<Guid>)command.Parameters[0]).TypedValue = claim.MessageId;
        ((NpgsqlParameter<Guid>)command.Parameters[1]).TypedValue = claim.Token;
        return command;
    }

    private static NpgsqlCommand CreateMutation(string sql)
    {
        var command = new NpgsqlCommand(sql);
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid));
        command.Parameters.Add(new NpgsqlParameter<Guid>("token", NpgsqlDbType.Uuid));
        return command;
    }

    public void Dispose()
    {
        _claim.Dispose();
        _complete.Dispose();
        _fail.Dispose();
        _failImmediately.Dispose();
    }
}
