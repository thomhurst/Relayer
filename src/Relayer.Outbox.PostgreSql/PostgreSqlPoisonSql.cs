namespace Relayer.Outbox.PostgreSql;

// One fenced statement owns the attempt increment, dead copy and ordered key barrier.
internal static class PostgreSqlPoisonSql
{
    // Retry after the native send operation has exhausted its own retries. Keep the deadline in
    // the row so a process restart cannot turn a broker outage into a hot retry loop.
    internal static string Retry(string table, string fenced, string now) => $"""
        WITH retried AS (
            UPDATE {table} SET attempts = LEAST(attempts, 29) + 1, last_error = @error,
                deliver_at = {Backoff(now)},
                claim_token = NULL, claim_expires_at = NULL
            WHERE {fenced} RETURNING id
        ) SELECT CASE WHEN EXISTS (SELECT 1 FROM retried) THEN 1 ELSE 0 END
        """;

    // One to 300 seconds, doubling per recorded attempt.
    private static string Backoff(string now) => $"{now} + LEAST(300, power(2, LEAST(attempts, 9))) * interval '1 second'";

    // Counted failures. With backoffFrom, a row that stays pending persists the same backoff as Retry,
    // so local handler retries cannot run hot on any dispatch path before they exhaust the limit.
    internal static string Failure(string table, string dead, string fenced, int limit, string? backoffFrom = null) => $"""
        WITH exhausted AS (
            DELETE FROM {table} WHERE {fenced} AND lane IS NULL AND attempts + 1 >= {limit}
            RETURNING *
        ), failed AS (
            UPDATE {table} SET attempts = attempts + 1, last_error = @error,
                status = CASE WHEN lane IS NOT NULL AND attempts + 1 >= {limit} THEN 'parked' ELSE status END,
                {(backoffFrom is null ? "" : $"deliver_at = CASE WHEN attempts + 1 >= {limit} THEN deliver_at ELSE {Backoff(backoffFrom)} END, ")}claim_token = NULL, claim_expires_at = NULL
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
