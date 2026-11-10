using System.Data;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

/// <summary>A durable inbox receipt, scoped to a consumer.</summary>
public sealed record PostgreSqlInboxReceipt(string Consumer, DateTimeOffset ProcessedAt);

/// <summary>Read-only durable metadata. Not present includes sends without retained history.</summary>
public sealed record PostgreSqlMessageLocation(
    Guid MessageId, string State, string? Destination, DateTimeOffset? DeliverAt,
    Guid? ClaimToken, DateTimeOffset? ClaimExpiresAt, int Attempts, string? LastError,
    DateTimeOffset? FailedAt, int? Lane, string? LaneOwner, DateTimeOffset? LaneExpiresAt,
    long? EarlierRows, IReadOnlyList<PostgreSqlInboxReceipt> InboxReceipts)
{
    /// <summary>Time durable completion was recorded after a successful native send, when retained.</summary>
    public DateTimeOffset? SentAt { get; init; }
}

/// <summary>Looks up message metadata in a consistent, read-only PostgreSQL snapshot. Never reads message bodies.</summary>
/// <remarks>The application owns the data source and authorization for the selected database/schema.</remarks>
public sealed class PostgreSqlMessageLookup
{
    private readonly NpgsqlDataSource _source;
    private readonly string _sql;

    /// <summary>Selects an application-owned data source and schema/prefix.</summary>
    public PostgreSqlMessageLookup(NpgsqlDataSource source, PostgreSqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        _source = source;
        options ??= new();
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        var schema = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + ".";
        string Table(string name) => schema + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + name);
        var table = Table("outbox");
        _sql = $"""
            SELECT CASE WHEN o.status = 'parked' OR EXISTS (SELECT 1 FROM {table} p WHERE p.lane = o.lane AND p.ordering_key = o.ordering_key AND p.seq < o.seq AND p.status = 'parked') THEN 'parked'
                        WHEN o.claim_expires_at > transaction_timestamp() THEN 'claimed'
                        WHEN o.deliver_at > transaction_timestamp() THEN 'scheduled' ELSE 'pending' END,
                   o.destination, o.deliver_at, o.claim_token, o.claim_expires_at, o.attempts, o.last_error,
                   NULL::timestamptz, o.lane, l.owner, l.expires_at,
                   (SELECT count(*) FROM {table} earlier WHERE earlier.lane = o.lane AND earlier.seq < o.seq), NULL::timestamptz
            FROM {table} o LEFT JOIN {Table("leases")} l ON l.name = 'lane:' || o.lane::text
            WHERE o.id = @id
            UNION ALL
            SELECT 'dead', d.destination, d.deliver_at, NULL::uuid, NULL::timestamptz, d.attempts, d.last_error,
                   d.failed_at, d.lane, l.owner, l.expires_at, NULL::bigint, NULL::timestamptz
            FROM {Table("outbox_dead")} d LEFT JOIN {Table("leases")} l ON l.name = 'lane:' || d.lane::text
            WHERE d.id = @id AND NOT EXISTS (SELECT 1 FROM {table} WHERE id = @id)
            UNION ALL
            SELECT 'sent', s.destination, NULL::timestamptz, NULL::uuid, NULL::timestamptz, 0, NULL::text,
                   NULL::timestamptz, NULL::integer, NULL::text, NULL::timestamptz, NULL::bigint, s.sent_at
            FROM {Table("outbox_sent")} s WHERE s.id = @id
                AND NOT EXISTS (SELECT 1 FROM {table} WHERE id = @id)
                AND NOT EXISTS (SELECT 1 FROM {Table("outbox_dead")} WHERE id = @id);
            SELECT consumer, processed_at FROM {Table("inbox")} WHERE message_id = @id ORDER BY consumer;
            """;
    }

    /// <summary>Returns current durable state and all recorded consumer receipts without changing any row.</summary>
    public async ValueTask<PostgreSqlMessageLocation> FindAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        if (messageId == Guid.Empty) throw new ArgumentException("Message ID must not be empty.", nameof(messageId));
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);
        await using (var readOnly = new NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction))
            await readOnly.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(_sql, connection, transaction);
        command.Parameters.AddWithValue("id", messageId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var receipts = new List<PostgreSqlInboxReceipt>();
        var result = new PostgreSqlMessageLocation(messageId, "not present", null, null, null, null, 0, null, null, null, null, null, null, receipts);
        // The dead query excludes live rows, including parked rows with a dead copy.
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            result = new(messageId, reader.GetString(0), reader.GetString(1), Read<DateTimeOffset>(reader, 2),
                Read<Guid>(reader, 3), Read<DateTimeOffset>(reader, 4), reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetString(6), Read<DateTimeOffset>(reader, 7),
                Read<int>(reader, 8), reader.IsDBNull(9) ? null : reader.GetString(9),
                Read<DateTimeOffset>(reader, 10), Read<long>(reader, 11), receipts) { SentAt = Read<DateTimeOffset>(reader, 12) };
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            receipts.Add(new(reader.GetString(0), reader.GetFieldValue<DateTimeOffset>(1)));
        return result;
    }

    private static T? Read<T>(NpgsqlDataReader reader, int ordinal) where T : struct =>
        reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<T>(ordinal);
}
