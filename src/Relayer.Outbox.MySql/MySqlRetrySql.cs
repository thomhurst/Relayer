namespace Relayer.Outbox.MySql;

// Native send retries finish before this durable transition. MySQL evaluates single-table
// UPDATE assignments left to right: calculate the delay from the previous attempt count first.
internal static class MySqlRetrySql
{
    internal static string Update(string table, string fenced, string now) => $"""
        UPDATE {table} SET deliver_at = TIMESTAMPADD(SECOND, LEAST(300, POW(2, LEAST(attempts, 9))), {now}),
            attempts = LEAST(attempts, 29) + 1, last_error = @error,
            claim_token = NULL, claim_expires_at = NULL WHERE {fenced}
        """;
}
