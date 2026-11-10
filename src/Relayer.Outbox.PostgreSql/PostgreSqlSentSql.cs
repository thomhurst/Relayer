namespace Relayer.Outbox.PostgreSql;

// Reuse PostgreSQL's fenced DELETE RETURNING, as poison completion does. The copy and delete
// succeed together; only rows deleted by this statement can become sent-history entries.
internal static class PostgreSqlSentSql
{
    internal static string Complete(string delete, string sent, string now, bool batch) => $"""
        WITH deleted AS ({delete} RETURNING row.id, row.destination), retained AS (
            INSERT INTO {sent} (id, destination, sent_at) SELECT id, destination, {now} FROM deleted
            ON CONFLICT (id) DO UPDATE SET destination = EXCLUDED.destination, sent_at = EXCLUDED.sent_at
            RETURNING true
        )
        SELECT {(batch ? "count(*)" : "true")} FROM retained
        """;
}
