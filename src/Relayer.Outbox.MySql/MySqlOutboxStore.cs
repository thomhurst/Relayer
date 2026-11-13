using System.Runtime.CompilerServices;
using System.Transactions;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

/// <summary>MySQL durable reads and atomic fenced mutations over the Relayer outbox table.</summary>
/// <remarks>
/// The caller owns the data source. Dispose this scoped store after use; operations must not overlap.
/// Operations require native autocommit and reject ambient transactions before opening a connection.
/// This does not enqueue messages, establish lane ownership or start dispatch and cleanup workers.
/// </remarks>
public sealed class MySqlOutboxStore : IOutboxStore, IDisposable
{
    private readonly MySqlDataSource _dataSource;
    private readonly string _failureSql;
    private readonly int _failureLimit;
    private readonly MySqlStoreParameters _read;
    private readonly MySqlStoreParameters _claim;
    private readonly MySqlStoreParameters _renew;
    private readonly MySqlStoreParameters _complete;
    private readonly MySqlStoreParameters _fail;
    private readonly MySqlStoreParameters _retry;
    private readonly MySqlStoreParameters _count;
    private readonly MySqlDispatchMutations _claimBatch;
    private readonly MySqlDispatchMutations _completeBatch;
    private readonly List<HeaderSlot> _headers = [];
    private readonly byte[] _readId = new byte[16];
    private bool _disposed;

    /// <summary>Uses an application-owned data source and snapshots the table prefix.</summary>
    public MySqlOutboxStore(MySqlDataSource dataSource, MySqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ValidateDataSource(dataSource);
        _dataSource = dataSource;
        var table = MySqlOutboxSchema.Table(options ?? new());
        var dead = MySqlOutboxSchema.DeadTable(options ?? new());
        _failureLimit = (options ?? new()).MaxDispatchAttempts;
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_failureLimit);
        const string eligible = "status = 'pending' AND (deliver_at IS NULL OR deliver_at <= @now) AND (claim_expires_at IS NULL OR claim_expires_at <= @now)";
        const string fenced = "id = @id AND claim_token = @token AND claim_expires_at > @now";
        _read = new($"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id FROM {table} WHERE {eligible} AND lane <=> CAST(@lane AS SIGNED) AND (@after IS NULL OR id > @after) ORDER BY id LIMIT @limit");
        _claim = new($"UPDATE {table} SET claim_token = @token, claim_expires_at = @expires WHERE id = @id AND {eligible}");
        _renew = new($"UPDATE {table} SET claim_expires_at = GREATEST(claim_expires_at, @expires) WHERE {fenced}");
        _complete = new($"DELETE FROM {table} WHERE {fenced}");
        _retry = new(MySqlRetrySql.Update(table, fenced, "@now"));
        _fail = new($"UPDATE {table} SET attempts=attempts+1,last_error=@error WHERE {fenced}");
        _failureSql = $"""
            REPLACE INTO {dead} (id,lane,ordering_key,transport,destination,message_type,headers,body,created_at,deliver_at,attempts,last_error,tenant_id,failed_at)
            SELECT id,lane,ordering_key,transport,destination,message_type,headers,body,created_at,deliver_at,attempts,last_error,tenant_id,UTC_TIMESTAMP(6)
            FROM {table} WHERE id=@id AND attempts>=@limit;
            DELETE FROM {table} WHERE id=@id AND attempts>=@limit AND lane IS NULL;
            UPDATE {table} SET status=IF(attempts>=@limit,'parked',status),claim_token=NULL,claim_expires_at=NULL WHERE id=@id;
            SELECT NOT EXISTS (SELECT 1 FROM {table} WHERE id=@id);
            """;
        _claimBatch = new($"UPDATE {table} SET claim_token = @token, claim_expires_at = @expires WHERE id = @id AND lane IS NULL AND {eligible}");
        _completeBatch = new($"DELETE FROM {table} WHERE {fenced}");
        _count = new($"SELECT COUNT(*) FROM {table} WHERE {eligible}");
    }

    internal static void ValidateDataSource(MySqlDataSource dataSource)
    {
        if (new MySqlConnectionStringBuilder(dataSource.ConnectionString).UseAffectedRows)
            throw new ArgumentException("MySQL outbox stores require UseAffectedRows=false so unchanged live claim renewals report ownership correctly.", nameof(dataSource));
    }

    /// <inheritdoc />
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken)
    {
        EnsureAvailable(cancellationToken);
        if (request.Lane < 0) throw new ArgumentOutOfRangeException(nameof(request), "Lane must not be negative.");
        if (destination.IsEmpty) return 0;
        _read.Now(request.Now);
        _read.Read(request.Lane, request.AfterId, destination.Length);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var command = _read.Command;
        command.Connection = connection;
        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            return await ReadRowsAsync(reader, destination, cancellationToken).ConfigureAwait(false);
        }
        finally { command.Connection = null; }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadRecoveryAsync(MySqlCommand command, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await ReadRowsAsync(reader, destination, cancellationToken, isolateMalformed: true).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadRowsAsync(MySqlDataReader reader, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken, bool includeOrderedSequence = false, bool isolateMalformed = false)
    {
        var count = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            reader.GetBytes(0, 0, _readId, 0, 16);
            var id = new Guid(_readId, bigEndian: true);
            try
            {
                if (_headers.Count == count) _headers.Add(new());
                var slot = _headers[count];
                var headers = DurableOutboxHeaderCodec.Read(reader.GetFieldValue<byte[]>(5), ref slot.Entries);
                if (includeOrderedSequence)
                    headers = OrderedSequenceHeader.Add(headers.Span, ref slot.OrderedEntries, slot.Sequence, reader.GetInt64(13));
                var transport = reader.GetString(1);
                var type = reader.GetString(2);
                var target = reader.GetString(3);
                if (isolateMalformed && (string.IsNullOrWhiteSpace(transport) || string.IsNullOrWhiteSpace(type) || string.IsNullOrWhiteSpace(target)))
                    throw new InvalidDataException("Transport, message type and destination must be nonempty.");
                var message = new OutgoingMessage(target, reader.GetFieldValue<byte[]>(4), headers,
                    reader.IsDBNull(6) ? null : new WireString(reader.GetString(6)),
                    reader.IsDBNull(7) ? null : Utc(reader.GetDateTime(7)));
                destination.Span[count++] = new(id, transport, type, message, Utc(reader.GetDateTime(8)),
                    reader.IsDBNull(9) ? null : reader.GetInt32(9), reader.GetInt32(10),
                    reader.IsDBNull(11) ? null : reader.GetString(11), reader.IsDBNull(12) ? null : reader.GetString(12));
            }
            catch (InvalidDataException error) when (isolateMalformed)
            {
                destination.Span[count++] = new(id, "", "", default, default, Attempts: reader.GetInt32(10), LastError: "Malformed outbox row: " + error.Message);
            }
        }
        return count;
    }

    internal ValueTask<FailureOutcome> QuarantineAsync(OutboxClaim claim, string error, CancellationToken cancellationToken) =>
        FailAsync(claim, error, DateTimeOffset.UtcNow, 1, cancellationToken);

    /// <inheritdoc />
    public ValueTask<bool> TryClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        _claim.Expires(expiresAt);
        return MutateAsync(_claim, claim, now, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> RenewClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        _renew.Expires(expiresAt);
        return MutateAsync(_renew, claim, now, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> CompleteAsync(OutboxClaim claim, DateTimeOffset now, CancellationToken cancellationToken)
    {
        Validate(claim);
        return MutateAsync(_complete, claim, now, cancellationToken);
    }

    /// <inheritdoc />
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> RecordFailureAsync(OutboxClaim claim, string error, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        Validate(claim);
        ArgumentNullException.ThrowIfNull(error);
        return (await RecordDispatchFailureAsync(claim, error, now, cancellationToken).ConfigureAwait(false)).Accepted;
    }

    internal ValueTask<FailureOutcome> RecordDispatchFailureAsync(OutboxClaim claim, string error, DateTimeOffset now,
        CancellationToken cancellationToken) => FailAsync(claim, error, now, _failureLimit, cancellationToken);

    // Native send failures remain pending, independently of the explicit poison budget.
    internal ValueTask<bool> RecordRetryAsync(OutboxClaim claim, string error, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        Validate(claim);
        ArgumentNullException.ThrowIfNull(error);
        _retry.Command.Parameters["error"].Value = error;
        return MutateAsync(_retry, claim, now, cancellationToken);
    }

    // Only returned after the native transaction commits; stale claims and rollback have no terminal outcome.
    internal readonly record struct FailureOutcome(bool Accepted, bool Removed)
    {
        internal void RecordTelemetry(RelayerInstrumentation? instrumentation)
        {
            if (!Removed || instrumentation is null) return;
            instrumentation.AddOutboxPending(-1);
            instrumentation.RecordOutboxPoison();
        }
    }

    // Native transaction makes the raw dead copy and live-row transition indivisible.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<FailureOutcome> FailAsync(OutboxClaim claim, string error, DateTimeOffset now, int limit,
        CancellationToken cancellationToken)
    {
        EnsureAvailable(cancellationToken);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var command = _fail.Command;
        _fail.Claim(claim);
        _fail.Now(now);
        command.Parameters["error"].Value = error;
        command.Connection = connection;
        command.Transaction = transaction;
        // Keep the claim until the raw copy and terminal transition finish under its row lock.
        var original = command.CommandText;
        try
        {
            if (await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1) return default;
            command.Parameters["limit"].Value = limit;
            command.CommandText = _failureSql;
            var removed = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) != 0;
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new(true, removed);
        }
        finally
        {
            command.CommandText = original;
            command.Transaction = null;
            command.Connection = null;
            command.Parameters["error"].Value = DBNull.Value;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> MutateAsync(MySqlStoreParameters operation, OutboxClaim claim, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        EnsureAvailable(cancellationToken);
        operation.Claim(claim);
        operation.Now(now);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var command = operation.Command;
        command.Connection = connection;
        try
        {
            // Native affected-row behavior can report zero when renewal does not change the expiry.
            // CLIENT_FOUND_ROWS (UseAffectedRows=false) is validated during registration/construction.
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
        }
        finally
        {
            command.Connection = null;
            command.Parameters["error"].Value = DBNull.Value;
        }
    }

    /// <inheritdoc />
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        EnsureAvailable(cancellationToken);
        _count.Now(now);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var command = _count.Command;
        command.Connection = connection;
        try { return (long)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!; }
        finally { command.Connection = null; }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ClaimBatchAsync(ReadOnlyMemory<OutboxClaim> claims, Memory<bool> claimed,
        DateTimeOffset now, DateTimeOffset expires, CancellationToken cancellationToken)
    {
        EnsureAvailable(cancellationToken);
        if (claimed.Length != claims.Length) throw new ArgumentException("Claims and outcomes must have equal lengths.", nameof(claimed));
        claimed.Span.Clear();
        if (claims.IsEmpty) return;
        ValidateBatch(claims.Span, now, expires);
        _claimBatch.Prepare(claims.Span, now, expires);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await _claimBatch.ExecuteAsync(connection, claimed, cancellationToken).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<long> CompleteBatchAsync(ReadOnlyMemory<OutboxClaim> claims,
        DateTimeOffset now, CancellationToken cancellationToken)
    {
        EnsureAvailable(cancellationToken);
        if (claims.IsEmpty) return 0;
        ValidateBatch(claims.Span);
        _completeBatch.Prepare(claims.Span, now, now);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await _completeBatch.ExecuteAsync(connection, default, cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateBatch(ReadOnlySpan<OutboxClaim> claims, DateTimeOffset? now = null, DateTimeOffset? expires = null)
    {
        foreach (var claim in claims) Validate(claim, now, expires);
    }

    private static DateTimeOffset Utc(DateTime value) => new(DateTime.SpecifyKind(value, DateTimeKind.Utc));

    private void EnsureAvailable(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (Transaction.Current is not null)
            throw new InvalidOperationException("MySQL outbox store operations require autocommit. Call outside an ambient TransactionScope.");
    }

    private static void Validate(OutboxClaim claim, DateTimeOffset? now = null, DateTimeOffset? expiresAt = null)
    {
        if (claim.MessageId == Guid.Empty || claim.Token == Guid.Empty)
            throw new ArgumentException("Outbox message ID and claim token must not be empty.", nameof(claim));
        if (expiresAt <= now) throw new ArgumentOutOfRangeException(nameof(expiresAt), "Claim expiry must be after now.");
    }

    /// <summary>Releases cached commands. Does not dispose the application data source.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _read.Command.Dispose();
        _claim.Command.Dispose();
        _renew.Command.Dispose();
        _complete.Command.Dispose();
        _fail.Command.Dispose();
        _retry.Command.Dispose();
        _count.Command.Dispose();
        _claimBatch.Dispose();
        _completeBatch.Dispose();
        _headers.Clear();
    }

    private sealed class HeaderSlot
    {
        internal KeyValuePair<string, WireString>[] Entries = [];
        internal KeyValuePair<string, WireString>[] OrderedEntries = [];
        internal byte[] Sequence { get; } = new byte[20];
    }
}
