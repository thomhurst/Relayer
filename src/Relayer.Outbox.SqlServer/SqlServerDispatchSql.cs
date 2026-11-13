namespace Relayer.Outbox.SqlServer;

internal static class SqlServerDispatchSql
{
    // Native clients own retries within a send. Persist the next eligible time after they fail,
    // using the existing claim fence and due-time barrier rather than an in-process retry loop.
    internal static string Retry(string table, string fenced, string now) => $"""
        UPDATE {table} SET attempts = CASE WHEN attempts < 30 THEN attempts + 1 ELSE 30 END,
            last_error = @error,
            deliver_at = DATEADD(second, CASE WHEN attempts >= 9 THEN 300 ELSE POWER(2, CASE WHEN attempts >= 9 THEN 0 ELSE attempts END) END, {now}),
            claim_token = NULL, claim_expires_at = NULL
        WHERE {fenced};
        SELECT CAST(@@ROWCOUNT AS bit);
        """;
}
