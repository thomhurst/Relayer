using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class OutboxConfiguration
{
    internal string Table { get; }
    internal int Lanes { get; }
    internal string InsertSql { get; }

    internal OutboxConfiguration(EntityFrameworkCoreOutboxOptions options)
    {
        // Reuse the schema's validation, including generated identifier lengths, at registration time.
        _ = PostgreSqlOutboxSchema.CreateScript(new() { Schema = options.Schema, TablePrefix = options.TablePrefix });
        if (options.Lanes <= 0) throw new ArgumentOutOfRangeException(nameof(options), "Lanes must be positive.");
        Table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        Lanes = options.Lanes;
        InsertSql = $"""
            INSERT INTO {Table}
                (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at, tenant_id)
            SELECT id, lane, convert_from(key, 'UTF8'), transport, destination, type, headers, body, created, delivery, tenant
            FROM unnest(@ids, @lanes, @keys, @transports, @destinations, @types, @headers, @bodies, @created, @delivery, @tenants)
                AS rows(id, lane, key, transport, destination, type, headers, body, created, delivery, tenant)
            """;
    }
}
