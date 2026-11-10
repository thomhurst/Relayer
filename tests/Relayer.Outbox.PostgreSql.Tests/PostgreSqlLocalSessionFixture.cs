using Npgsql;
using TUnit.Core.Interfaces;

namespace Relayer.Outbox.PostgreSql.Tests;

// One server per test session. Each local-transport scenario owns a separate database,
// including the database used by its external crash/recovery processes.
public sealed class PostgreSqlLocalSessionFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly PostgreSqlFixture _server = new();
    public Task InitializeAsync() => _server.StartAsync();
    public Database CreateDatabase() => new(_server.ConnectionString,
        TestContext.Current!.Isolation.GetIsolatedName("local"));
    public ValueTask DisposeAsync() => _server.DisposeAsync();

    public sealed class Database(string adminConnectionString, string name) : IAsyncDisposable
    {
        private bool _created;
        public string ConnectionString { get; private set; } = "";
        public async Task StartAsync()
        {
            await using var admin = NpgsqlDataSource.Create(adminConnectionString);
            await using var command = admin.CreateCommand("CREATE DATABASE " + PostgreSqlOutboxSchema.QuoteIdentifier(name));
            await command.ExecuteNonQueryAsync();
            _created = true;
            ConnectionString = new NpgsqlConnectionStringBuilder(adminConnectionString) { Database = name }.ConnectionString;
        }
        public async ValueTask DisposeAsync()
        {
            if (!_created) return;
            _created = false;
            await using var admin = NpgsqlDataSource.Create(adminConnectionString);
            await using var command = admin.CreateCommand("DROP DATABASE " + PostgreSqlOutboxSchema.QuoteIdentifier(name) + " WITH (FORCE)");
            await command.ExecuteNonQueryAsync();
        }
    }
}
