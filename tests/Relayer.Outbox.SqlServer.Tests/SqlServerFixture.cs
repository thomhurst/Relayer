using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Relayer.TestFixtures;
using TUnit.Core.Interfaces;

namespace Relayer.Outbox.SqlServer.Tests;

// One container per test session; each test uses its own schema and prefix.
public sealed class SqlServerFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly string _name = "relayer-sqlserver-" + Guid.NewGuid().ToString("N");
    private readonly DockerCommand _docker = new(TimeSpan.FromMinutes(3));
    private bool _started;
    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        _started = true;
        try
        {
            await _docker.RunAsync(["run", "--detach", "--rm", "--name", _name, "--publish", "127.0.0.1::1433",
                "--env", "ACCEPT_EULA=Y", "--env", "MSSQL_SA_PASSWORD=Relayer-test-2026!",
                "mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04"]);
            var port = (await _docker.RunAsync(["port", _name, "1433/tcp"])).Trim().Split(':')[^1];
            ConnectionString = $"Server=127.0.0.1,{port};User ID=sa;Password=Relayer-test-2026!;Encrypt=False;Pool Blocking Period=NeverBlock";
            // Short readiness attempts must not become the timeout for concurrent test logins.
            var readinessConnectionString = new SqlConnectionStringBuilder(ConnectionString)
            {
                ConnectTimeout = 2,
                Pooling = false
            }.ConnectionString;
            var deadline = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    await using var connection = new SqlConnection(readinessConnectionString);
                    await connection.OpenAsync();
                    return;
                }
                catch (SqlException) when (deadline.Elapsed < TimeSpan.FromSeconds(90))
                {
                    await Task.Delay(200);
                }
            }
        }
        catch (Exception exception)
        {
            try { await DisposeAsync(); }
            catch (Exception cleanupFailure) { exception.Data["CleanupFailure"] = cleanupFailure; }
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_started) return;
        _started = false;
        using (var pool = new SqlConnection(ConnectionString)) SqlConnection.ClearPool(pool);
        await _docker.RunAsync(["rm", "--force", "--volumes", _name]);
    }
}
