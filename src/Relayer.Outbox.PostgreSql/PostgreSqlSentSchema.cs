using Npgsql;

namespace Relayer.Outbox.PostgreSql;

// Provisioning stays application-owned. Validate before any native send can be accepted.
internal static class PostgreSqlSentSchema
{
    internal static async Task ValidateAsync(NpgsqlDataSource source, bool retain, string? schema,
        string prefix, CancellationToken cancellationToken)
    {
        if (!retain) return;
        var table = PostgreSqlOutboxSchema.QuoteIdentifier(schema ?? "public") + "." +
            PostgreSqlOutboxSchema.QuoteIdentifier(prefix + "outbox_sent");
        await using var command = source.CreateCommand("SELECT to_regclass(@table) IS NOT NULL");
        command.Parameters.AddWithValue("table", table);
        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not true)
            throw new InvalidOperationException($"RetainSentMessages requires PostgreSQL table {table}. Run PostgreSqlOutboxSchema.CreateScript for this schema and table prefix before starting dispatch.");
    }
}
