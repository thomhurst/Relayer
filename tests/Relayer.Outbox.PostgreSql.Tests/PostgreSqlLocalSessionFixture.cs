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
