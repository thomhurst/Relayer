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
        var server = await PostgreSqlSession.Server;
        _admin = PostgreSqlSession.Admin;
        await Execute($"CREATE DATABASE {Database}");
        ConnectionString = server.ConnectionString.Replace("Database=postgres", "Database=" + Database);
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
    private static readonly Lazy<Task<PostgreSqlServer>> Shared = new(StartAsync);
    private static readonly List<Exception> CleanupFailures = [];

    internal static Task<PostgreSqlServer> Server => Shared.Value;

    // Pooled, unlike the tests' own connections: creating and dropping databases should not add connection churn.
    internal static NpgsqlDataSource? Admin { get; private set; }

    private static async Task<PostgreSqlServer> StartAsync()
    {
        var server = new PostgreSqlServer(new DockerCommand().RunAsync, failure =>
        {
            Console.Error.WriteLine(failure);
            CleanupFailures.Add(failure);
        });
        try
        {
            await server.StartAsync();
            Admin = NpgsqlDataSource.Create(server.ConnectionString.Replace("Pooling=false", "Pooling=true"));
        }
        catch
        {
            await server.DisposeAsync();
            throw;
        }
        return server;
    }

    [After(TestSession)]
    public static async Task StopAsync()
    {
        if (!Shared.IsValueCreated) return;
        // A failed startup already removed its container and failed the tests that needed it.
        if (Shared.Value.IsCompletedSuccessfully)
        {
            await Admin!.DisposeAsync();
            await Shared.Value.Result.DisposeAsync();
        }
        if (CleanupFailures.Count > 0) throw new AggregateException("PostgreSQL server cleanup failed.", CleanupFailures);
    }
}
