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
    private readonly string _read;
    private readonly string _claim;
    private readonly string _renew;
    private readonly string _complete;
    private readonly string _fail;
    private readonly string _count;
    private readonly byte[] _id = new byte[16];
    private readonly byte[] _token = new byte[16];
    // SqlParameter.Value accepts only object. Reuse exactly typed boxes at this native boundary;
    // instances are scoped and operations must not overlap. No per-operation value-type boxing.
    private readonly object _now = default(DateTimeOffset);
    private readonly object _expires = default(DateTimeOffset);
    private readonly object _lane = 0;
    private readonly object _limit = 0;
    private readonly List<RowBuffer> _rows = [];

    /// <summary>Uses the native connection string unchanged and snapshots explicit schema identifiers.</summary>
    [RequiresUnreferencedCode("Microsoft.Data.SqlClient 7.1.1 retains reflection-based authentication, configuration and diagnostics. Trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("Microsoft.Data.SqlClient 7.1.1 is not warning-clean under Native AOT; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public SqlServerOutboxStore(string connectionString, SqlServerOutboxOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = new SqlConnectionStringBuilder(connectionString);
        _connectionString = connectionString;
        var table = SqlServerOutboxSchema.Table(options ?? new());
        var eligible = "status = 'pending' AND (deliver_at IS NULL OR deliver_at <= @now) AND (claim_expires_at IS NULL OR claim_expires_at <= @now)";
        var fenced = "id = @id AND claim_token = @token AND claim_expires_at > @now";
        _read = $"SELECT TOP (@limit) id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id FROM {table} WHERE {eligible} AND (lane = @lane OR (lane IS NULL AND @lane IS NULL)) AND (@after IS NULL OR id > @after) ORDER BY id";
        _claim = $"UPDATE {table} SET claim_token = @token, claim_expires_at = @expires OUTPUT CAST(1 AS bit) WHERE id = @id AND {eligible}";
        _renew = $"UPDATE {table} SET claim_expires_at = CASE WHEN claim_expires_at > @expires THEN claim_expires_at ELSE @expires END OUTPUT CAST(1 AS bit) WHERE {fenced}";
        _complete = $"DELETE FROM {table} OUTPUT CAST(1 AS bit) WHERE {fenced}";
        _fail = $"UPDATE {table} SET attempts = attempts + 1, last_error = @error, claim_token = NULL, claim_expires_at = NULL OUTPUT CAST(1 AS bit) WHERE {fenced}";
        _count = $"SELECT COUNT_BIG(*) FROM {table} WHERE {eligible}";
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken)
    {
        if (request.Lane < 0) throw new ArgumentOutOfRangeException(nameof(request), "Lane must not be negative.");
        CheckOperation(cancellationToken);
        if (destination.IsEmpty) return 0;
        using var connection = new SqlConnection(_connectionString);
        using var command = new SqlCommand(_read, connection);
        Unsafe.Unbox<DateTimeOffset>(_now) = request.Now.ToUniversalTime();
        Unsafe.Unbox<int>(_lane) = request.Lane.GetValueOrDefault();
        Unsafe.Unbox<int>(_limit) = destination.Length;
        Add(command, "@now", SqlDbType.DateTimeOffset, _now);
        Add(command, "@lane", SqlDbType.Int, request.Lane.HasValue ? _lane : null);
        if (request.AfterId is { } after) after.TryWriteBytes(_id, bigEndian: true, out _);
        Add(command, "@after", SqlDbType.Binary, request.AfterId.HasValue ? _id : null, 16);
        Add(command, "@limit", SqlDbType.Int, _limit);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var count = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_rows.Count == count) _rows.Add(new());
            var slot = _rows[count];
            reader.GetBytes(0, 0, _id, 0, 16);
            var id = new Guid(_id, bigEndian: true);
            var body = ReadBytes(reader, 4, ref slot.Body);
            var headers = DurableOutboxHeaderCodec.Read(ReadBytes(reader, 5, ref slot.Headers), ref slot.Entries);
            var message = new OutgoingMessage(reader.GetString(3), body, headers,
                reader.IsDBNull(6) ? null : new WireString(reader.GetString(6)),
                reader.IsDBNull(7) ? null : reader.GetDateTimeOffset(7));
            destination.Span[count++] = new(id, reader.GetString(1), reader.GetString(2), message,
                reader.GetDateTimeOffset(8), reader.IsDBNull(9) ? null : reader.GetInt32(9), reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetString(11), reader.IsDBNull(12) ? null : reader.GetString(12));
        }
        return count;
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> MutateAsync(string sql, OutboxClaim claim, DateTimeOffset now,
        DateTimeOffset? expiresAt, string? error, CancellationToken cancellationToken)
    {
        CheckOperation(cancellationToken);
        claim.MessageId.TryWriteBytes(_id, bigEndian: true, out _);
        claim.Token.TryWriteBytes(_token, bigEndian: true, out _);
        using var connection = new SqlConnection(_connectionString);
        using var command = new SqlCommand(sql, connection);
        Add(command, "@id", SqlDbType.Binary, _id, 16);
        Add(command, "@token", SqlDbType.Binary, _token, 16);
        Unsafe.Unbox<DateTimeOffset>(_now) = now.ToUniversalTime();
        Add(command, "@now", SqlDbType.DateTimeOffset, _now);
        if (expiresAt.HasValue)
        {
            Unsafe.Unbox<DateTimeOffset>(_expires) = expiresAt.Value.ToUniversalTime();
            Add(command, "@expires", SqlDbType.DateTimeOffset, _expires);
        }
        if (error is not null) Add(command, "@error", SqlDbType.NVarChar, error, -1);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }

    /// <inheritdoc />
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        CheckOperation(cancellationToken);
        using var connection = new SqlConnection(_connectionString);
        using var command = new SqlCommand(_count, connection);
        Unsafe.Unbox<DateTimeOffset>(_now) = now.ToUniversalTime();
        Add(command, "@now", SqlDbType.DateTimeOffset, _now);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
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
        internal byte[] Body = [];
        internal byte[] Headers = [];
        internal KeyValuePair<string, WireString>[] Entries = [];
    }
}
