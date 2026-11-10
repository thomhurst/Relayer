using Npgsql;

namespace Relayer.Outbox.PostgreSql.Tests;

// One database per fixture on the session's shared server, so tests run in parallel
// without seeing each other's tables, sessions or replication slots.
internal sealed class PostgreSqlFixture : IAsyncDisposable
{
    private NpgsqlDataSource? _admin;
    public string Database { get; } = TestContext.Current!.Isolation.GetIsolatedName(Guid.NewGuid().ToString("N")[..8]);
    public string ConnectionString { get; private set; } = "";

    public async Task StartAsync()
    {
        var session = await PostgreSqlSession.Current;
        _admin = session.Admin;
        await Execute($"CREATE DATABASE {Database}");
        ConnectionString = session.Server.ConnectionString.Replace("Database=postgres", "Database=" + Database);
    }

    public async ValueTask DisposeAsync()
    {
        if (_admin is null) return;
        try
        {
            // Includes databases a test created with this one's name as their prefix.
            var databases = new List<string>();
            await using (var command = _admin.CreateCommand("SELECT datname FROM pg_database WHERE starts_with(datname, $1)"))
            {
                command.Parameters.AddWithValue(Database);
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync()) databases.Add(reader.GetString(0));
            }
            foreach (var database in databases) await Execute($"DROP DATABASE {database} WITH (FORCE)");
        }
        catch (Exception exception)
        {
            // Throwing here would replace an exception unwinding through await using.
            var failure = new InvalidOperationException($"Could not drop PostgreSQL test database {Database}.", exception);
            Console.Error.WriteLine(failure);
            TestContext.Current!.StateBag.GetOrAdd("PostgreSqlFixture.CleanupFailures", static _ => new List<Exception>()).Add(failure);
        }
    }

    private async Task Execute(string sql)
    {
        await using var command = _admin!.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }
}

public static class PostgreSqlSession
{
    // Started by the first test that needs a database, so filtered runs of pure unit tests start no container.
    private static Lazy<Task<Session>> _shared = new(StartAsync);

    internal static Task<Session> Current => Volatile.Read(ref _shared).Value;

    // Admin is pooled, unlike the tests' own connections: creating and dropping databases should not add connection churn.
    internal sealed record Session(PostgreSqlServer Server, NpgsqlDataSource Admin, List<Exception> CleanupFailures);

    private static async Task<Session> StartAsync()
    {
        var failures = new List<Exception>();
        var server = new PostgreSqlServer(new DockerCommand().RunAsync, failure =>
        {
            Console.Error.WriteLine(failure);
            failures.Add(failure);
        });
        try
        {
            await server.StartAsync();
            return new(server, NpgsqlDataSource.Create(server.ConnectionString.Replace("Pooling=false", "Pooling=true")), failures);
        }
        catch
        {
            await server.DisposeAsync();
            throw;
        }
    }

    [After(TestSession)]
    public static async Task StopAsync()
    {
        // Like TUnit's own per-session instances, a later session in this process starts afresh.
        var shared = Interlocked.Exchange(ref _shared, new(StartAsync));
        // A failed startup already removed its container and failed the tests that needed it.
        if (!shared.IsValueCreated || !shared.Value.IsCompletedSuccessfully) return;
        var session = shared.Value.Result;
        await session.Admin.DisposeAsync();
        await session.Server.DisposeAsync();
        if (session.CleanupFailures.Count > 0) throw new AggregateException("PostgreSQL server cleanup failed.", session.CleanupFailures);
    }
}
