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
        InsertSql = PostgreSqlOutboxBatch.CreateInsertSql(Table);
    }
}
