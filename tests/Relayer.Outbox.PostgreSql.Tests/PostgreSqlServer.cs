using System.Diagnostics;
using Npgsql;

namespace Relayer.Outbox.PostgreSql.Tests;

// The session's PostgreSQL container. Tests reach it through PostgreSqlFixture, which gives each its own database.
internal sealed class PostgreSqlServer : IAsyncDisposable
{
    private readonly string _name = "relayer-schema-test-" + Guid.NewGuid().ToString("N");
    private readonly Func<string[], Task<string>> _docker;
    private readonly Action<Exception> _reportCleanupFailure;
    private bool _startupAttempted;
    public string ConnectionString { get; private set; } = "";

    internal PostgreSqlServer(Func<string[], Task<string>> docker, Action<Exception> reportCleanupFailure)
    {
        _docker = docker;
        _reportCleanupFailure = reportCleanupFailure;
    }

    public async Task StartAsync()
    {
        // The daemon can create the container even if the CLI never returns its ID.
        _startupAttempted = true;
        try
        {
            await _docker(["run", "--detach", "--rm", "--name", _name, "--publish", "127.0.0.1::5432",
                "--env", "POSTGRES_PASSWORD=relayer-test", "postgres:18-alpine",
                // Every test shares this server: allow logical replication and their combined sessions.
                "-c", "wal_level=logical", "-c", "max_connections=1000",
                "-c", "max_replication_slots=100", "-c", "max_wal_senders=100",
                // Dropping a database forces a checkpoint. The data is disposable, so do not sync it to disk.
                "-c", "fsync=off"]);
            var port = (await _docker(["port", _name, "5432/tcp"])).Trim().Split(':')[^1];
            // This container has neither TLS nor Kerberos. Native encryption fallback can
            // reconnect after authentication fails and replace the original failure.
            ConnectionString = $"Host=127.0.0.1;Port={port};Username=postgres;Password=relayer-test;Database=postgres;Pooling=false;Timeout=2;SSL Mode=Disable;GSS Encryption Mode=Disable";
            var timeout = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    await using var connection = new NpgsqlConnection(ConnectionString);
                    await connection.OpenAsync();
                    return;
                }
                catch (NpgsqlException) when (timeout.Elapsed < TimeSpan.FromSeconds(60))
                {
                    await Task.Delay(200);
                }
            }
        }
        catch (Exception exception)
        {
            exception.Data["DockerContainerName"] = _name;
            Console.Error.WriteLine($"PostgreSQL fixture startup failed. After Docker recovers, clean up with: docker rm --force --volumes {_name}");
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_startupAttempted) return;
        _startupAttempted = false;
        try
        {
            await _docker(["rm", "--force", "--volumes", _name]);
        }
        catch (Exception exception)
        {
            var failure = new InvalidOperationException(
                $"PostgreSQL fixture cleanup failed. After Docker recovers, run: docker rm --force --volumes {_name}", exception);
            // Throwing here would replace an exception unwinding through await using.
            _reportCleanupFailure(failure);
        }
    }
}
