using System.Buffers;
using System.Diagnostics;
using MySqlConnector;
using Relayer.TestFixtures;

namespace Relayer.Outbox.MySql.Tests;

// One database per fixture on the session's shared server, so tests run in parallel
// without seeing each other's tables, locks or named-lock resources.
internal sealed class MySqlFixture : IAsyncDisposable
{
    private MySqlDataSource? _admin;
    private MySqlDataSource? _source;
    internal string Database { get; } = TestContext.Current!.Isolation.GetIsolatedName(Guid.NewGuid().ToString("N")[..8]);
    internal MySqlDataSource Source => _source!;
    internal MySqlOutboxOptions Options { get; } = new();

    internal async Task StartAsync()
    {
        var session = await MySqlSession.Current;
        _admin = session.Admin;
        await using (var create = _admin.CreateCommand($"CREATE DATABASE {Database}")) await create.ExecuteNonQueryAsync();
        _source = new(session.Server.ConnectionString + ";Database=" + Database);
        await Execute(MySqlOutboxSchema.CreateScript(Options));
    }

    internal async Task Execute(string sql)
    {
        await using var command = Source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    internal async Task Insert(Guid id, DateTimeOffset now, int? lane = null, DateTimeOffset? deliverAt = null,
        string status = "pending", DateTimeOffset? expires = null)
    {
        var headers = new ArrayBufferWriter<byte>();
        DurableOutboxHeaderCodec.Write(headers, [new("duplicate", new("é")), new("duplicate", new(new byte[] { 255, 0, 254 })), new("empty", new(""))]);
        await using var connection = await Source.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            INSERT INTO {MySqlOutboxSchema.Table(Options)}
            (id, transport, destination, message_type, body, headers, created_at, lane, deliver_at, status, claim_token, claim_expires_at,
                ordering_key, tenant_id)
            VALUES (@id, 'events', 'orders', 'order;v=1', @body, @headers, @now, @lane, @delivery, @status, @token, @expires,
                'customer-é', 'tenant-é')
            """;
        command.Parameters.AddWithValue("id", id.ToByteArray(bigEndian: true));
        command.Parameters.AddWithValue("body", new byte[] { 0, 1, 255 });
        command.Parameters.AddWithValue("headers", headers.WrittenMemory);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("lane", lane);
        command.Parameters.AddWithValue("delivery", deliverAt);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("token", expires.HasValue ? Guid.NewGuid().ToByteArray(bigEndian: true) : null);
        command.Parameters.AddWithValue("expires", expires);
        await command.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_admin is null) return;
        try
        {
            if (_source is not null) await _source.DisposeAsync();
            await using var drop = _admin.CreateCommand($"DROP DATABASE IF EXISTS {Database}");
            await drop.ExecuteNonQueryAsync();
        }
        catch (Exception exception)
        {
            // Throwing here would replace an exception unwinding through await using.
            var failure = new InvalidOperationException($"Could not drop MySQL test database {Database}.", exception);
            Console.Error.WriteLine(failure);
            TestContext.Current!.StateBag.GetOrAdd("MySqlFixture.CleanupFailures", static _ => new List<Exception>()).Add(failure);
        }
    }
}

// The session's MySQL container. Tests reach it through MySqlFixture.
internal sealed class MySqlServer : IAsyncDisposable
{
    private readonly string _name = "relayer-mysql-test-" + Guid.NewGuid().ToString("N");
    private readonly DockerCommand _docker = new();
    private bool _started;
    internal string ConnectionString { get; private set; } = "";

    internal async Task StartAsync()
    {
        _started = true;
        await _docker.RunAsync(["run", "--detach", "--rm", "--name", _name, "--publish", "127.0.0.1::3306",
            "--env", "MYSQL_ROOT_PASSWORD=relayer-test", "--env", "MYSQL_ROOT_HOST=%", "mysql:8.4",
            // Every test shares this server: allow their combined sessions.
            "--max-connections=1000"]);
        var port = (await _docker.RunAsync(["port", _name, "3306/tcp"])).Trim().Split(':')[^1];
        // Non-default GUID and DateTime settings prove the store owns its wire representation,
        // without overriding application configuration on the source.
        ConnectionString = $"Server=127.0.0.1;Port={port};User ID=root;Password=relayer-test;Connection Timeout=2;SslMode=Disabled;GuidFormat=LittleEndianBinary16;DateTimeKind=Local";
        await using var probe = new MySqlDataSource(ConnectionString + ";Pooling=false");
        var timeout = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                await using var connection = await probe.OpenConnectionAsync();
                return;
            }
            catch (MySqlException) when (timeout.Elapsed < TimeSpan.FromSeconds(60)) { await Task.Delay(200); }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_started) return;
        _started = false;
        await _docker.RunAsync(["rm", "--force", "--volumes", _name]);
    }
}

public static class MySqlSession
{
    // Started by the first test that needs a database, so filtered runs of pure unit tests start no container.
    private static Lazy<Task<Session>> _shared = new(StartAsync);

    internal static Task<Session> Current => Volatile.Read(ref _shared).Value;

    internal sealed record Session(MySqlServer Server, MySqlDataSource Admin);

    private static async Task<Session> StartAsync()
    {
        var server = new MySqlServer();
        try
        {
            await server.StartAsync();
            return new(server, new(server.ConnectionString));
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
        await shared.Value.Result.Admin.DisposeAsync();
        await shared.Value.Result.Server.DisposeAsync();
    }
}

public static class MySqlFixtureCleanup
{
    [AfterEvery(Test)]
    public static void ReportFailures(TestContext context)
    {
        if (context.StateBag.TryGetValue<List<Exception>>("MySqlFixture.CleanupFailures", out var failures) && failures.Count > 0)
            throw new AggregateException("MySQL fixture cleanup failed.", failures);
    }
}
