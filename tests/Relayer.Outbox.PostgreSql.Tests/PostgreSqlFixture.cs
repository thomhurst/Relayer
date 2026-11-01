using System.Diagnostics;
using Npgsql;

namespace Relayer.Outbox.PostgreSql.Tests;

internal sealed class PostgreSqlFixture : IAsyncDisposable
{
    private readonly string _name = "relayer-schema-test-" + Guid.NewGuid().ToString("N");
    private bool _started;
    public string ConnectionString { get; private set; } = "";

    public async Task StartAsync()
    {
        await Docker("run", "--detach", "--rm", "--name", _name, "--publish", "127.0.0.1::5432",
            "--env", "POSTGRES_PASSWORD=relayer-test", "postgres:18-alpine");
        _started = true;
        var port = (await Docker("port", _name, "5432/tcp")).Trim().Split(':')[^1];
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

    public async ValueTask DisposeAsync()
    {
        if (_started) await Docker("rm", "--force", _name);
    }

    private static async Task<string> Docker(params string[] args)
    {
        var start = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in args) start.ArgumentList.Add(arg);
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        var errorText = await error;
        if (process.ExitCode != 0) throw new InvalidOperationException($"docker {args[0]} failed: {errorText}");
        return await output;
    }
}
