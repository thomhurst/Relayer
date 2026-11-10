namespace Relayer.Outbox.PostgreSql;

// One fenced statement owns the attempt increment, dead copy and ordered key barrier.
internal static class PostgreSqlPoisonSql
{
    internal static string Failure(string table, string dead, string fenced, int limit) => $"""
        WITH exhausted AS (
            DELETE FROM {table} WHERE {fenced} AND lane IS NULL AND attempts + 1 >= {limit}
            RETURNING *
        ), failed AS (
            UPDATE {table} SET attempts = attempts + 1, last_error = @error,
                status = CASE WHEN lane IS NOT NULL AND attempts + 1 >= {limit} THEN 'parked' ELSE status END,
                claim_token = NULL, claim_expires_at = NULL
            WHERE {fenced} AND (lane IS NOT NULL OR attempts + 1 < {limit}) RETURNING *
        ), copied AS (
            INSERT INTO {dead} (id, lane, ordering_key, transport, destination, message_type, headers, body,
                created_at, deliver_at, attempts, last_error, tenant_id, failed_at)
            SELECT id, lane, ordering_key, transport, destination, message_type, headers, body,
                created_at, deliver_at, attempts, last_error, tenant_id, clock_timestamp()
            FROM failed WHERE attempts >= {limit}
            UNION ALL
            SELECT id, lane, ordering_key, transport, destination, message_type, headers, body,
                created_at, deliver_at, attempts + 1, @error, tenant_id, clock_timestamp()
            FROM exhausted RETURNING id
        )
        SELECT CASE WHEN EXISTS (SELECT 1 FROM copied) THEN 2
            WHEN EXISTS (SELECT 1 FROM failed) THEN 1 ELSE 0 END
        """;
}
