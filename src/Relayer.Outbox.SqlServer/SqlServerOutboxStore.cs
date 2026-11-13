using Relayer.Transports;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Transactions;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer;

/// <summary>Bounded SQL Server reads and atomic fenced mutations over an explicitly deployed outbox.</summary>
/// <remarks>
/// Instances are scoped and do not support concurrent operations. Finish using borrowed rows before the next read.
/// Native pooling owns connections; no enqueue, inbox, ordered ownership or workers are registered.
/// </remarks>
public sealed class SqlServerOutboxStore : IOutboxStore
{
    private readonly string _connectionString;
    private readonly int _failureLimit;
    private readonly string _read;
    private SqlConnection? _dispatchConnection;
    private SqlTransaction? _dispatchTransaction;
    internal void UseDispatchTransaction(SqlConnection? connection, SqlTransaction? transaction)
    {
        _dispatchConnection = connection;
        _dispatchTransaction = transaction;
    }
    private readonly string _claim;
    private readonly string _renew;
    private readonly string _complete;
    private readonly string _fail;
    private readonly string _retry;
    private readonly string _count;
    private readonly string _batchClaim;
    private readonly string _batchComplete;
    private readonly byte[] _id = new byte[16];
    private readonly byte[] _token = new byte[16];
    // SqlParameter.Value accepts only object. Reuse exactly typed boxes at this native boundary;
    // instances are scoped and operations must not overlap. No per-operation value-type boxing.
    private readonly object _now = default(DateTimeOffset);
    private readonly object _expires = default(DateTimeOffset);
    private readonly object _lane = 0;
    private readonly object _limit = 0;
    private readonly List<RowBuffer> _rows = [];
    private readonly List<ClaimBuffer> _batchClaims = [];

    /// <summary>Uses the native connection string unchanged and snapshots explicit schema identifiers.</summary>
    [RequiresUnreferencedCode("Microsoft.Data.SqlClient 7.1.1 retains reflection-based authentication, configuration and diagnostics. Trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("Microsoft.Data.SqlClient 7.1.1 is not warning-clean under Native AOT; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public SqlServerOutboxStore(string connectionString, SqlServerOutboxOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = new SqlConnectionStringBuilder(connectionString);
        _connectionString = connectionString;
        options ??= new();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxDispatchAttempts);
        _failureLimit = options.MaxDispatchAttempts;
        var table = SqlServerOutboxSchema.Table(options);
        var dead = SqlServerOutboxSchema.DeadTable(options);
        var eligible = "status = 'pending' AND (deliver_at IS NULL OR deliver_at <= @now) AND (claim_expires_at IS NULL OR claim_expires_at <= @now)";
        var fenced = "id = @id AND claim_token = @token AND claim_expires_at > @now";
        _read = $"SELECT TOP (@limit) id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id FROM {table} WHERE {eligible} AND (lane = @lane OR (lane IS NULL AND @lane IS NULL)) AND (@after IS NULL OR id > @after) ORDER BY id";
        _claim = $"UPDATE {table} SET claim_token = @token, claim_expires_at = @expires OUTPUT CAST(1 AS bit) WHERE id = @id AND {eligible}";
        _renew = $"UPDATE {table} SET claim_expires_at = CASE WHEN claim_expires_at > @expires THEN claim_expires_at ELSE @expires END OUTPUT CAST(1 AS bit) WHERE {fenced}";
        _complete = $"DELETE FROM {table} OUTPUT CAST(1 AS bit) WHERE {fenced}";
        _fail = $"""
            SET XACT_ABORT ON;
            DECLARE @own bit = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END;
            IF @own = 1 BEGIN TRANSACTION;
            DECLARE @changed int;
            UPDATE {table} SET attempts = attempts + 1, last_error = @error,
                status = CASE WHEN lane IS NOT NULL THEN 'parked' ELSE status END,
                claim_token = NULL, claim_expires_at = NULL
            OUTPUT inserted.id, inserted.lane, inserted.ordering_key, inserted.transport, inserted.destination,
                inserted.message_type, inserted.headers, inserted.body, inserted.created_at, inserted.deliver_at,
                inserted.attempts, inserted.last_error, inserted.tenant_id, SYSUTCDATETIME()
            INTO {dead} (id, lane, ordering_key, transport, destination, message_type, headers, body,
                created_at, deliver_at, attempts, last_error, tenant_id, failed_at)
            WHERE {fenced} AND attempts + 1 >= @maxAttempts;
            SET @changed = @@ROWCOUNT;
            IF @changed = 1
                DELETE FROM {table} WHERE id = @id AND lane IS NULL;
            ELSE
            BEGIN
                UPDATE {table} SET attempts = attempts + 1, last_error = @error,
                    claim_token = NULL, claim_expires_at = NULL WHERE {fenced};
                SET @changed = @@ROWCOUNT;
            END;
            IF @own = 1 COMMIT;
            SELECT CAST(@changed AS bit);
            """;
        _retry = SqlServerDispatchSql.Retry(table, fenced, "@now");
        _count = $"SELECT COUNT_BIG(*) FROM {table} WHERE {eligible}";
        _batchClaim = _claim.Replace(" OUTPUT CAST(1 AS bit)", "", StringComparison.Ordinal) + "; SELECT CAST(@@ROWCOUNT AS bit)";
        _batchComplete = _complete.Replace(" OUTPUT CAST(1 AS bit)", "", StringComparison.Ordinal) + "; SELECT CAST(@@ROWCOUNT AS bigint)";
    }

    /// <inheritdoc />
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken)
    {
        if (request.Lane < 0) throw new ArgumentOutOfRangeException(nameof(request), "Lane must not be negative.");
        CheckOperation(cancellationToken);
        if (destination.IsEmpty) return 0;
        using var ownedConnection = _dispatchConnection is null ? new SqlConnection(_connectionString) : null;
        var connection = _dispatchConnection ?? ownedConnection!;
        using var command = new SqlCommand(_read, connection, _dispatchTransaction);
        Unsafe.Unbox<DateTimeOffset>(_now) = request.Now.ToUniversalTime();
        Unsafe.Unbox<int>(_lane) = request.Lane.GetValueOrDefault();
        Unsafe.Unbox<int>(_limit) = destination.Length;
        Add(command, "@now", SqlDbType.DateTimeOffset, _now);
        Add(command, "@lane", SqlDbType.Int, request.Lane.HasValue ? _lane : null);
        if (request.AfterId is { } after) after.TryWriteBytes(_id, bigEndian: true, out _);
        Add(command, "@after", SqlDbType.Binary, request.AfterId.HasValue ? _id : null, 16);
        Add(command, "@limit", SqlDbType.Int, _limit);
        if (ownedConnection is not null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await ReadRowsAsync(reader, destination, false, cancellationToken).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadRowsAsync(SqlCommand command, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken, bool includeOrderedSequence = false)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        // Ordered rows must reach fenced per-row failure handling even when stored headers are malformed.
        return await ReadRowsAsync(reader, destination, includeOrderedSequence, cancellationToken, includeOrderedSequence).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ReadRecoveryAsync(SqlCommand command, Memory<OutboxRecord> destination, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await ReadRowsAsync(reader, destination, true, cancellationToken).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> ReadRowsAsync(SqlDataReader reader, Memory<OutboxRecord> destination,
        bool isolateMalformed, CancellationToken cancellationToken, bool includeOrderedSequence = false)
    {
        var count = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_rows.Count == count) _rows.Add(new());
            var slot = _rows[count];
            reader.GetBytes(0, 0, _id, 0, 16);
            var id = new Guid(_id, bigEndian: true);
            try
            {
                var body = ReadBytes(reader, 4, ref slot.Body);
                var headers = DurableOutboxHeaderCodec.Read(ReadBytes(reader, 5, ref slot.Headers), ref slot.Entries);
                if (includeOrderedSequence)
                    headers = OrderedSequenceHeader.Add(headers.Span, ref slot.OrderedEntries, slot.Sequence, reader.GetInt64(13));
                var transport = reader.GetString(1);
                var messageType = reader.GetString(2);
                var destinationName = reader.GetString(3);
                if (isolateMalformed)
                {
                    if (string.IsNullOrWhiteSpace(transport)) throw new InvalidDataException("Outbox transport name is empty.");
                    if (string.IsNullOrWhiteSpace(messageType)) throw new InvalidDataException("Outbox message type is empty.");
                    if (string.IsNullOrWhiteSpace(destinationName)) throw new InvalidDataException("Outbox destination is empty.");
                }
                var message = new OutgoingMessage(destinationName, body, headers,
                    reader.IsDBNull(6) ? null : new WireString(reader.GetString(6)),
                    reader.IsDBNull(7) ? null : reader.GetDateTimeOffset(7));
                destination.Span[count++] = new(id, transport, messageType, message,
                    reader.GetDateTimeOffset(8), reader.IsDBNull(9) ? null : reader.GetInt32(9), reader.GetInt32(10),
                    reader.IsDBNull(11) ? null : reader.GetString(11), reader.IsDBNull(12) ? null : reader.GetString(12));
            }
            catch (InvalidDataException error) when (isolateMalformed)
            {
                // Routing stays on the durable row; a malformed projection is never sent.
                destination.Span[count++] = new(id, "", "", default, reader.GetDateTimeOffset(8),
                    reader.IsDBNull(9) ? null : reader.GetInt32(9), reader.GetInt32(10),
                    "Malformed outbox row: " + error.Message,
                    reader.IsDBNull(12) ? null : reader.GetString(12));
            }
        }
        return count;
    }

    internal ValueTask<bool> QuarantineAsync(OutboxClaim claim, string error, CancellationToken cancellationToken)
    {
        Validate(claim);
        ArgumentNullException.ThrowIfNull(error);
        return MutateAsync(_fail, claim, DateTimeOffset.UtcNow, null, error, cancellationToken, failureLimit: 1);
    }

    /// <inheritdoc />
    public ValueTask<bool> TryClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        return MutateAsync(_claim, claim, now, expiresAt, null, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> RenewClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        Validate(claim, now, expiresAt);
        return MutateAsync(_renew, claim, now, expiresAt, null, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> CompleteAsync(OutboxClaim claim, DateTimeOffset now, CancellationToken cancellationToken)
    {
        Validate(claim);
        return MutateAsync(_complete, claim, now, null, null, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> RecordFailureAsync(OutboxClaim claim, string error, DateTimeOffset now, CancellationToken cancellationToken)
    {
        Validate(claim);
        ArgumentNullException.ThrowIfNull(error);
        return MutateAsync(_fail, claim, now, null, error, cancellationToken);
    }

    internal ValueTask<bool> RecordRetryAsync(OutboxClaim claim, string error, DateTimeOffset now, CancellationToken cancellationToken)
    {
        Validate(claim);
        ArgumentNullException.ThrowIfNull(error);
        return MutateAsync(_retry, claim, now, null, error, cancellationToken);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> MutateAsync(string sql, OutboxClaim claim, DateTimeOffset now,
        DateTimeOffset? expiresAt, string? error, CancellationToken cancellationToken, int? failureLimit = null)
    {
        CheckOperation(cancellationToken);
        claim.MessageId.TryWriteBytes(_id, bigEndian: true, out _);
        claim.Token.TryWriteBytes(_token, bigEndian: true, out _);
        using var ownedConnection = _dispatchConnection is null ? new SqlConnection(_connectionString) : null;
        var connection = _dispatchConnection ?? ownedConnection!;
        using var command = new SqlCommand(sql, connection, _dispatchTransaction);
        Add(command, "@id", SqlDbType.Binary, _id, 16);
        Add(command, "@token", SqlDbType.Binary, _token, 16);
        Unsafe.Unbox<DateTimeOffset>(_now) = now.ToUniversalTime();
        Add(command, "@now", SqlDbType.DateTimeOffset, _now);
        if (expiresAt.HasValue)
        {
            Unsafe.Unbox<DateTimeOffset>(_expires) = expiresAt.Value.ToUniversalTime();
            Add(command, "@expires", SqlDbType.DateTimeOffset, _expires);
        }
        if (error is not null)
        {
            Add(command, "@error", SqlDbType.NVarChar, error, -1);
            Add(command, "@maxAttempts", SqlDbType.Int, failureLimit ?? _failureLimit);
        }
        if (ownedConnection is not null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    // Native RPC batches retain one independently fenced autocommit statement per row.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ClaimBatchAsync(ReadOnlyMemory<OutboxClaim> claims, Memory<bool> claimed,
        DateTimeOffset now, DateTimeOffset expiresAt, CancellationToken cancellationToken)
    {
        CheckOperation(cancellationToken);
        if (claimed.Length < claims.Length) throw new ArgumentException("The claim result buffer is too small.", nameof(claimed));
        if (expiresAt <= now) throw new ArgumentOutOfRangeException(nameof(expiresAt));
        claimed.Span.Clear();
        if (claims.IsEmpty) return;
        using var ownedConnection = _dispatchConnection is null ? new SqlConnection(_connectionString) : null;
        var connection = _dispatchConnection ?? ownedConnection!;
        await using var batch = new SqlBatch(connection, _dispatchTransaction);
        PrepareBatch(batch, claims.Span, _batchClaim, now, expiresAt);
        if (ownedConnection is not null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        for (var i = 0; i < claims.Length; i++)
        {
            if (i != 0 && !await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException("SQL Server did not return every batched claim result.");
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException("SQL Server returned an empty batched claim result.");
            claimed.Span[i] = reader.GetBoolean(0);
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<long> CompleteBatchAsync(ReadOnlyMemory<OutboxClaim> claims, DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        CheckOperation(cancellationToken);
        if (claims.IsEmpty) return 0;
        using var ownedConnection = _dispatchConnection is null ? new SqlConnection(_connectionString) : null;
        var connection = _dispatchConnection ?? ownedConnection!;
        await using var batch = new SqlBatch(connection, _dispatchTransaction);
        PrepareBatch(batch, claims.Span, _batchComplete, now, null);
        if (ownedConnection is not null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        long completed = 0;
        for (var i = 0; i < claims.Length; i++)
        {
            if (i != 0 && !await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException("SQL Server did not return every batched completion result.");
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidDataException("SQL Server returned an empty batched completion result.");
            completed += reader.GetInt64(0);
        }
        return completed;
    }

    private void PrepareBatch(SqlBatch batch, ReadOnlySpan<OutboxClaim> claims, string sql, DateTimeOffset now, DateTimeOffset? expiresAt)
    {
        Unsafe.Unbox<DateTimeOffset>(_now) = now.ToUniversalTime();
        Unsafe.Unbox<DateTimeOffset>(_expires) = expiresAt.GetValueOrDefault().ToUniversalTime();
        for (var i = 0; i < claims.Length; i++)
        {
            Validate(claims[i]);
            if (_batchClaims.Count == i) _batchClaims.Add(new());
            var slot = _batchClaims[i];
            claims[i].MessageId.TryWriteBytes(slot.Id, bigEndian: true, out _);
            claims[i].Token.TryWriteBytes(slot.Token, bigEndian: true, out _);
            var command = new SqlBatchCommand(sql);
            command.Parameters.Add(new SqlParameter("@id", SqlDbType.Binary, 16) { Value = slot.Id });
            command.Parameters.Add(new SqlParameter("@token", SqlDbType.Binary, 16) { Value = slot.Token });
            command.Parameters.Add(new SqlParameter("@now", SqlDbType.DateTimeOffset) { Value = _now });
            if (expiresAt.HasValue) command.Parameters.Add(new SqlParameter("@expires", SqlDbType.DateTimeOffset) { Value = _expires });
            batch.BatchCommands.Add(command);
        }
    }

    private sealed class ClaimBuffer
    {
        internal readonly byte[] Id = new byte[16];
        internal readonly byte[] Token = new byte[16];
    }

    /// <inheritdoc />
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        CheckOperation(cancellationToken);
        using var ownedConnection = _dispatchConnection is null ? new SqlConnection(_connectionString) : null;
        var connection = _dispatchConnection ?? ownedConnection!;
        using var command = new SqlCommand(_count, connection, _dispatchTransaction);
        Unsafe.Unbox<DateTimeOffset>(_now) = now.ToUniversalTime();
        Add(command, "@now", SqlDbType.DateTimeOffset, _now);
        if (ownedConnection is not null) await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return (long)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }

    private static void CheckOperation(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Transaction.Current is not null)
            throw new InvalidOperationException("SQL Server outbox store operations require autocommit. Call outside an ambient TransactionScope.");
    }

    private static void Validate(OutboxClaim claim, DateTimeOffset? now = null, DateTimeOffset? expiresAt = null)
    {
        if (claim.MessageId == Guid.Empty || claim.Token == Guid.Empty)
            throw new ArgumentException("Outbox message ID and claim token must not be empty.", nameof(claim));
        if (expiresAt <= now) throw new ArgumentOutOfRangeException(nameof(expiresAt), "Claim expiry must be after now.");
    }

    private static void Add(SqlCommand command, string name, SqlDbType type, object? value, int size = 0) =>
        command.Parameters.Add(new SqlParameter(name, type, size) { Value = value ?? DBNull.Value });

    private static ReadOnlyMemory<byte> ReadBytes(SqlDataReader reader, int ordinal, ref byte[] buffer)
    {
        var length = checked((int)reader.GetBytes(ordinal, 0, null, 0, 0));
        if (buffer.Length < length) Array.Resize(ref buffer, length);
        if (reader.GetBytes(ordinal, 0, buffer, 0, length) != length)
            throw new InvalidDataException("Truncated SQL Server outbox payload.");
        return buffer.AsMemory(0, length);
    }

    private sealed class RowBuffer
    {
        internal readonly byte[] Sequence = new byte[20];
        internal KeyValuePair<string, WireString>[] OrderedEntries = [];
        internal byte[] Body = [];
        internal byte[] Headers = [];
        internal KeyValuePair<string, WireString>[] Entries = [];
    }
}
