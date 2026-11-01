using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;

namespace Relayer.Outbox.PostgreSql;

/// <summary>PostgreSQL durable reads and fenced mutations over the shared Relayer schema.</summary>
/// <remarks>
/// The caller owns the data source. Instances are scoped and do not support concurrent operations.
/// This does not enqueue messages, start dispatch workers, or establish ordered lane ownership.
/// </remarks>
public sealed class PostgreSqlOutboxStore : IOutboxStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _read;
    private readonly string _claim;
    private readonly string _claimBatch;
    private readonly string _renew;
    private readonly string _complete;
    private readonly string _completeBatch;
    private readonly string _fail;
    private readonly string _count;
    private readonly ScalarOperation<bool> _mutation = new(static value => value is true);
    private readonly ScalarOperation<long?> _pendingCount = new(static value => (long?)value);
    private readonly ScalarOperation<long> _completedCount = new(static value => (long)value!);
    private readonly List<Guid> _batchIds = [];
    private readonly List<Guid> _batchTokens = [];
    private readonly List<HeaderSlot> _headers = [];

    /// <summary>Uses an application-owned data source and snapshots the schema/table prefix.</summary>
    public PostgreSqlOutboxStore(NpgsqlDataSource dataSource, PostgreSqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        options ??= new();
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        var table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        var eligible = "status = 'pending' AND (deliver_at IS NULL OR deliver_at <= @now) AND (claim_expires_at IS NULL OR claim_expires_at <= @now)";
        var fenced = "id = @id AND claim_token = @token AND claim_expires_at > @now";
        _read = $"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id FROM {table} WHERE {eligible} AND lane IS NOT DISTINCT FROM @lane AND (@after IS NULL OR id > @after) ORDER BY id LIMIT @limit";
        _claim = $"UPDATE {table} SET claim_token = @token, claim_expires_at = @expires WHERE id = @id AND {eligible} RETURNING true";
        _claimBatch = $"UPDATE {table} AS row SET claim_token = claims.token, claim_expires_at = @expires FROM unnest(@ids, @tokens) WITH ORDINALITY AS claims(id, token, ordinal) WHERE row.id = claims.id AND {eligible} AND lane IS NULL RETURNING claims.ordinal";
        _renew = $"UPDATE {table} SET claim_expires_at = GREATEST(claim_expires_at, @expires) WHERE {fenced} RETURNING true";
        _complete = $"DELETE FROM {table} WHERE {fenced} RETURNING true";
        _completeBatch = $"WITH deleted AS (DELETE FROM {table} AS row USING unnest(@ids, @tokens) AS claims(id, token) WHERE row.id = claims.id AND row.claim_token = claims.token AND row.claim_expires_at > @now RETURNING row.id) SELECT count(*) FROM deleted";
        _fail = $"UPDATE {table} SET attempts = attempts + 1, last_error = @error, claim_token = NULL, claim_expires_at = NULL WHERE {fenced} RETURNING true";
        _count = $"SELECT count(*) FROM {table} WHERE {eligible}";
    }

    /// <inheritdoc />
    public async ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken)
    {
        if (request.Lane < 0) throw new ArgumentOutOfRangeException(nameof(request), "Lane must not be negative.");
        cancellationToken.ThrowIfCancellationRequested();
        if (destination.IsEmpty) return 0;
        using var command = _dataSource.CreateCommand(_read);
        Add(command, "now", NpgsqlDbType.TimestampTz, request.Now.ToUniversalTime());
        Add(command, "lane", NpgsqlDbType.Integer, request.Lane);
        Add(command, "after", NpgsqlDbType.Uuid, request.AfterId);
        Add(command, "limit", NpgsqlDbType.Integer, destination.Length);
        return await ReadRowsAsync(command, destination, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadRowsAsync(NpgsqlCommand command, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var count = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_headers.Count == count) _headers.Add(new());
            var slot = _headers[count];
            var headers = OutboxHeaderCodec.Read(reader.GetFieldValue<byte[]>(5), ref slot.Entries);
            var message = new OutgoingMessage(reader.GetString(3), reader.GetFieldValue<byte[]>(4), headers,
                reader.IsDBNull(6) ? null : new WireString(reader.GetString(6)),
                reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7));
            destination.Span[count++] = new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), message,
                reader.GetFieldValue<DateTimeOffset>(8), reader.IsDBNull(9) ? null : reader.GetInt32(9), reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetString(11), reader.IsDBNull(12) ? null : reader.GetString(12));
        }
        return count;
    }

    /// <inheritdoc />
    public ValueTask<bool> TryClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        var command = CreateMutation(_claim, claim, now);
        Add(command, "expires", NpgsqlDbType.TimestampTz, expiresAt.ToUniversalTime());
        return _mutation.Start(command, cancellationToken);
    }

    // Direct dispatch claims one native group in one statement. Returned flags refer to input positions,
    // regardless of PostgreSQL's result order; live competing claims and ineligible rows stay untouched.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ClaimBatchAsync(ReadOnlyMemory<OutboxClaim> claims, Memory<bool> claimed,
        DateTimeOffset now, DateTimeOffset expiresAt, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (claimed.Length != claims.Length) throw new ArgumentException("One result slot is required per claim.", nameof(claimed));
        foreach (var claim in claims.Span) Validate(claim, now, expiresAt);
        claimed.Span.Clear();
        if (claims.IsEmpty) return;
        using var command = CreateBatchMutation(_claimBatch, claims, now);
        Add(command, "expires", NpgsqlDbType.TimestampTz, expiresAt.ToUniversalTime());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            claimed.Span[(int)reader.GetInt64(0) - 1] = true;
    }

    /// <inheritdoc />
    public ValueTask<bool> RenewClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        var command = CreateMutation(_renew, claim, now);
        Add(command, "expires", NpgsqlDbType.TimestampTz, expiresAt.ToUniversalTime());
        return _mutation.Start(command, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> CompleteAsync(OutboxClaim claim, DateTimeOffset now, CancellationToken cancellationToken)
    {
        Validate(claim);
        return _mutation.Start(CreateMutation(_complete, claim, now), cancellationToken);
    }

    /// <summary>Deletes sent rows in one statement, fencing each row by its matching, live claim.</summary>
    /// <remarks>Returns the number deleted. Expired, absent and stale claims do not delete rows.</remarks>
    public ValueTask<long> CompleteBatchAsync(ReadOnlyMemory<OutboxClaim> claims, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (claims.IsEmpty) return new(0L);
        foreach (var claim in claims.Span) Validate(claim);
        return _completedCount.Start(CreateBatchMutation(_completeBatch, claims, now), cancellationToken);
    }

    private NpgsqlCommand CreateBatchMutation(string sql, ReadOnlyMemory<OutboxClaim> claims, DateTimeOffset now)
    {
        _batchIds.Clear();
        _batchTokens.Clear();
        foreach (var claim in claims.Span)
        {
            _batchIds.Add(claim.MessageId);
            _batchTokens.Add(claim.Token);
        }
        var command = _dataSource.CreateCommand(sql);
        Add(command, "ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, _batchIds);
        Add(command, "tokens", NpgsqlDbType.Array | NpgsqlDbType.Uuid, _batchTokens);
        Add(command, "now", NpgsqlDbType.TimestampTz, now.ToUniversalTime());
        return command;
    }

    /// <inheritdoc />
    public ValueTask<bool> RecordFailureAsync(OutboxClaim claim, string error, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        Validate(claim);
        ArgumentNullException.ThrowIfNull(error);
        var command = CreateMutation(_fail, claim, now);
        Add(command, "error", NpgsqlDbType.Text, error);
        return _mutation.Start(command, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        var command = _dataSource.CreateCommand(_count);
        Add(command, "now", NpgsqlDbType.TimestampTz, now.ToUniversalTime());
        return _pendingCount.Start(command, cancellationToken);
    }

    private NpgsqlCommand CreateMutation(string sql, OutboxClaim claim, DateTimeOffset now)
    {
        var command = _dataSource.CreateCommand(sql);
        Add(command, "id", NpgsqlDbType.Uuid, claim.MessageId);
        Add(command, "token", NpgsqlDbType.Uuid, claim.Token);
        Add(command, "now", NpgsqlDbType.TimestampTz, now.ToUniversalTime());
        return command;
    }

    private static void Add<T>(NpgsqlCommand command, string name, NpgsqlDbType type, T value) =>
        command.Parameters.Add(new NpgsqlParameter<T>(name, type) { TypedValue = value });

    private static void Validate(OutboxClaim claim, DateTimeOffset? now = null, DateTimeOffset? expiresAt = null)
    {
        if (claim.MessageId == Guid.Empty || claim.Token == Guid.Empty)
            throw new ArgumentException("Outbox message ID and claim token must not be empty.", nameof(claim));
        if (expiresAt <= now) throw new ArgumentOutOfRangeException(nameof(expiresAt), "Claim expiry must be after now.");
    }

    private sealed class HeaderSlot
    {
        internal KeyValuePair<string, WireString>[] Entries = [];
    }
}
