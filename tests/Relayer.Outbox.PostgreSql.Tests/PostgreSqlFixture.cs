using System.Diagnostics;
using Npgsql;

namespace Relayer.Outbox.PostgreSql.Tests;

internal sealed class PostgreSqlFixture : IAsyncDisposable
{
    private readonly string _name = "relayer-schema-test-" + Guid.NewGuid().ToString("N");
    private readonly Func<string[], Task<string>> _docker;
    private readonly Action<Exception> _reportCleanupFailure;
    private bool _startupAttempted;
    public string ConnectionString { get; private set; } = "";

    public PostgreSqlFixture() : this(new DockerCommand().RunAsync, ReportCleanupFailure) { }

    internal PostgreSqlFixture(Func<string[], Task<string>> docker, Action<Exception> reportCleanupFailure)
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
                "--env", "POSTGRES_PASSWORD=relayer-test", "postgres:18-alpine"]);
            var port = (await _docker(["port", _name, "5432/tcp"])).Trim().Split(':')[^1];
            ConnectionString = $"Host=127.0.0.1;Port={port};Username=postgres;Password=relayer-test;Database=postgres;Pooling=false;Timeout=2";
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

    private static void ReportCleanupFailure(Exception failure)
    {
        Console.Error.WriteLine(failure);
        var context = TestContext.Current ?? throw new InvalidOperationException("Fixture cleanup requires a TUnit test context.", failure);
        var failures = context.StateBag.GetOrAdd("PostgreSqlFixture.CleanupFailures", static _ => new List<Exception>());
        failures.Add(failure);
    }
}
