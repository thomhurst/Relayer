using Npgsql;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Durable outbox failure details available through a dead-letter snapshot's Features.</summary>
public sealed record PostgreSqlDeadLetterDetails(OutboxRecord Record, DateTimeOffset FailedAt);

internal sealed class PostgreSqlDeadLetterManager : IDeadLetterManager
{
    private readonly NpgsqlDataSource _source;
    private readonly string _id;
    private readonly string _table;
    private readonly string _dead;
    private readonly IReadOnlyList<DeadLetterSource> _listed;

    internal PostgreSqlDeadLetterManager(string name, NpgsqlDataSource source, PostgreSqlOutboxOptions options)
    {
        _source = source;
        _id = name;
        var schema = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + ".";
        _table = schema + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        _dead = schema + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox_dead");
        _listed = Array.AsReadOnly(new[] { new DeadLetterSource(name, name) });
    }

    public IReadOnlyList<DeadLetterSource> List() => _listed;

    public async ValueTask<DeadLetterPage> PeekAsync(string source, int maxMessages = 20, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        Validate(source, maxMessages);
        Guid? after = null;
        if (cursor is not null)
            after = Guid.TryParseExact(cursor, "D", out var parsed) ? parsed : throw new ArgumentException("Invalid PostgreSQL dead-letter cursor.", nameof(cursor));
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var messages = await ReadAsync(connection, null, maxMessages, after, cancellationToken).ConfigureAwait(false);
        return new(messages.AsReadOnly(), messages.Count == 0 ? null : messages[^1].MessageId);
    }

    public ValueTask<int> RedriveAsync(string source, Func<DeadLetterMessage, bool>? filter = null,
        int maxMessages = 20, CancellationToken cancellationToken = default) => MaintainAsync(source, filter, maxMessages, false, cancellationToken);

    public ValueTask<int> PurgeAsync(string source, int maxMessages = 20, CancellationToken cancellationToken = default)
        => MaintainAsync(source, null, maxMessages, true, cancellationToken);

    private async ValueTask<int> MaintainAsync(string source, Func<DeadLetterMessage, bool>? filter, int maxMessages,
        bool purge, CancellationToken cancellationToken)
    {
        Validate(source, maxMessages);
        await using var connection = await _source.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        // Lock bounded dead rows, then restore their original durable rows. Never send from management.
        var messages = await ReadAsync(connection, transaction, maxMessages, null, cancellationToken).ConfigureAwait(false);
        var changed = 0;
        foreach (var message in messages)
        {
            if (!purge && filter is not null && !filter(message)) continue;
            var row = message.Features.GetRequired<PostgreSqlDeadLetterDetails>().Record;
            var sql = purge
                ? $"DELETE FROM {_table} WHERE id = @id AND status = 'parked'"
                : row.Lane is not null
                    ? $"UPDATE {_table} SET status = 'pending', attempts = 0, last_error = NULL WHERE id = @id AND status = 'parked'"
                    : $"INSERT INTO {_table} (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at, tenant_id) SELECT id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at, tenant_id FROM {_dead} WHERE id = @id";
            using (var command = new NpgsqlCommand(sql, connection, transaction))
            {
                command.Parameters.AddWithValue("id", row.Id);
                var affected = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                if (!purge && affected != 1) throw new InvalidOperationException($"Ordered dead row {row.Id:D} has no parked barrier. Restore its durable row at the original sequence before redrive, or explicitly purge the dead row to discard it. The redrive batch was rolled back.");
            }
            using (var command = new NpgsqlCommand($"DELETE FROM {_dead} WHERE id = @id", connection, transaction))
            {
                command.Parameters.AddWithValue("id", row.Id);
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            changed++;
        }
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return changed;
    }

    private async ValueTask<List<DeadLetterMessage>> ReadAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        int limit, Guid? after, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand($"SELECT id, transport, message_type, destination, body, headers, ordering_key, deliver_at, created_at, lane, attempts, last_error, tenant_id, failed_at FROM {_dead} WHERE (@after IS NULL OR id > @after) ORDER BY id LIMIT @limit" +
            (transaction is null ? "" : " FOR UPDATE SKIP LOCKED"), connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<Guid?>("after", NpgsqlTypes.NpgsqlDbType.Uuid) { TypedValue = after });
        command.Parameters.AddWithValue("limit", limit);
        var messages = new List<DeadLetterMessage>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            KeyValuePair<string, WireString>[] entries = [];
            var headers = OutboxHeaderCodec.Read(reader.GetFieldValue<byte[]>(5), ref entries);
            var outgoing = new OutgoingMessage(reader.GetString(3), reader.GetFieldValue<byte[]>(4), headers,
                reader.IsDBNull(6) ? null : new WireString(reader.GetString(6)),
                reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7));
            var row = new OutboxRecord(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), outgoing,
                reader.GetFieldValue<DateTimeOffset>(8), reader.IsDBNull(9) ? null : reader.GetInt32(9), reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetString(11), reader.IsDBNull(12) ? null : reader.GetString(12));
            var snapshot = new DeadLetterMessage { MessageId = row.Id.ToString("D"), Body = outgoing.Body };
            snapshot.Features.Set(new PostgreSqlDeadLetterDetails(row, reader.GetFieldValue<DateTimeOffset>(13)));
            messages.Add(snapshot);
        }
        return messages;
    }

    private void Validate(string source, int limit)
    {
        if (!StringComparer.Ordinal.Equals(source, _id)) throw new ArgumentException("Unknown PostgreSQL outbox dead-letter source.", nameof(source));
        if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit));
    }
}
