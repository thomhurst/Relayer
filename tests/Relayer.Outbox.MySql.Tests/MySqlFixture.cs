using System.Buffers;
using System.Diagnostics;
using MySqlConnector;
using Relayer.TestFixtures;

namespace Relayer.Outbox.MySql.Tests;

internal sealed class MySqlFixture : IAsyncDisposable
{
    private readonly string _name = "relayer-mysql-test-" + Guid.NewGuid().ToString("N");
    private readonly DockerCommand _docker = new();
    private bool _started;
    private MySqlDataSource? _source;
    internal MySqlDataSource Source => _source!;
    internal MySqlOutboxOptions Options { get; } = new();

    internal async Task StartAsync()
    {
        _started = true;
        await _docker.RunAsync(["run", "--detach", "--rm", "--name", _name, "--publish", "127.0.0.1::3306",
            "--env", "MYSQL_ROOT_PASSWORD=relayer-test", "--env", "MYSQL_ROOT_HOST=%",
            "--env", "MYSQL_DATABASE=relayer", "mysql:8.4"]);
        var port = (await _docker.RunAsync(["port", _name, "3306/tcp"])).Trim().Split(':')[^1];
        // Non-default GUID and DateTime settings prove the store owns its wire representation,
        // without overriding application configuration on the source.
        _source = new($"Server=127.0.0.1;Port={port};User ID=root;Password=relayer-test;Database=relayer;Connection Timeout=2;SslMode=Disabled;GuidFormat=LittleEndianBinary16;DateTimeKind=Local");
        var timeout = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                await using var connection = await Source.OpenConnectionAsync();
                break;
            }
            catch (MySqlException) when (timeout.Elapsed < TimeSpan.FromSeconds(60)) { await Task.Delay(200); }
        }
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
        try { if (_source is not null) await _source.DisposeAsync(); }
        finally
        {
            if (_started)
            {
                _started = false;
                try { await _docker.RunAsync(["rm", "--force", "--volumes", _name]); }
                catch (Exception exception)
                {
                    var failure = new InvalidOperationException($"MySQL fixture cleanup failed. Run: docker rm --force --volumes {_name}", exception);
                    Console.Error.WriteLine(failure);
                    var context = TestContext.Current ?? throw failure;
                    context.StateBag.GetOrAdd("MySqlFixture.CleanupFailures", static _ => new List<Exception>()).Add(failure);
                }
            }
        }
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
