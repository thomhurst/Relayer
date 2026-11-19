using Npgsql;

namespace Relayer.Outbox.PostgreSql.Tests;

// One server per test session. Each local-transport scenario owns a separate database,
// including the database used by its external crash/recovery processes.
public sealed class PostgreSqlLocalSessionFixture
{
    public Database CreateDatabase() => new(TestContext.Current!.Isolation.GetIsolatedName("local"));

    public sealed class Database(string name) : IAsyncDisposable
    {
        private string _adminConnectionString = "";
        private bool _created;
        public string ConnectionString { get; private set; } = "";
        public async Task StartAsync()
        {
            _adminConnectionString = (await PostgreSqlSession.Current).Server.ConnectionString;
            await using var admin = NpgsqlDataSource.Create(_adminConnectionString);
            await using var command = admin.CreateCommand("CREATE DATABASE " + PostgreSqlOutboxSchema.QuoteIdentifier(name));
            await command.ExecuteNonQueryAsync();
            _created = true;
            ConnectionString = new NpgsqlConnectionStringBuilder(_adminConnectionString) { Database = name }.ConnectionString;
        }
        public async Task DisconnectAsync(CancellationToken cancellationToken)
        {
            await SetAllowConnectionsAsync(false, cancellationToken);

            // ALLOW_CONNECTIONS only affects new sessions. Terminate existing ones on this database.
            await using var admin = NpgsqlDataSource.Create(_adminConnectionString);
            await using var command = admin.CreateCommand("""
                SELECT coalesce(bool_and(pg_terminate_backend(pid, 5000)), true)
                FROM pg_stat_activity
                WHERE datname = @database
                """);
            command.Parameters.AddWithValue("database", name);
            if (await command.ExecuteScalarAsync(cancellationToken) is not true)
                throw new InvalidOperationException("PostgreSQL did not terminate all sessions on the test database.");
        }

        public Task ReconnectAsync(CancellationToken cancellationToken) =>
            SetAllowConnectionsAsync(true, cancellationToken);

        private async Task SetAllowConnectionsAsync(bool allow, CancellationToken cancellationToken)
        {
            await using var admin = NpgsqlDataSource.Create(_adminConnectionString);
            var database = PostgreSqlOutboxSchema.QuoteIdentifier(name);
            await using var command = admin.CreateCommand($"ALTER DATABASE {database} ALLOW_CONNECTIONS {(allow ? "true" : "false")}");
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            if (!_created) return;
            _created = false;
            await using var admin = NpgsqlDataSource.Create(_adminConnectionString);
            await using var command = admin.CreateCommand("DROP DATABASE " + PostgreSqlOutboxSchema.QuoteIdentifier(name) + " WITH (FORCE)");
            await command.ExecuteNonQueryAsync();
        }
    }
}
