using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class MaintenanceTests
{
    [Test]
    public async Task Registration_binds_named_snapshots_and_validates_limits()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["BatchSize"] = "7", ["Schema"] = "custom", ["DeadRetention"] = "infinite", ["InboxRetention"] = "2.00:00:00"
        }).Build();
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerPostgreSqlMaintenance("a", source, configuration);
        services.AddRelayerPostgreSqlMaintenance("b", source, new PostgreSqlMaintenanceOptions { BatchSize = 9 });
        configuration["BatchSize"] = "99";
        await using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>();
        await Assert.That(monitor.Get("a").BatchSize).IsEqualTo(7);
        await Assert.That(monitor.Get("a").Schema).IsEqualTo("custom");
        await Assert.That(monitor.Get("a").DeadRetention).IsNull();
        await Assert.That(monitor.Get("a").InboxRetention).IsEqualTo(TimeSpan.FromDays(2));
        await Assert.That(monitor.Get("b").BatchSize).IsEqualTo(9);
        _ = new PostgreSqlMaintenanceOptions { DeadRetention = TimeSpan.FromDays(365), InboxRetention = TimeSpan.FromDays(365) }.Validate();
        await Assert.That(provider.GetServices<IHostedService>().Count()).IsEqualTo(2);
        await Assert.That(() => services.AddRelayerPostgreSqlMaintenance("a", source)).Throws<InvalidOperationException>();
        await Assert.That(() => new PostgreSqlMaintenanceOptions { BatchSize = 0 }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlMaintenanceOptions { RecoveryDelay = TimeSpan.FromSeconds(-1) }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlMaintenanceOptions { ClaimDuration = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlMaintenanceOptions { SweepInterval = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlMaintenanceOptions { CleanupInterval = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new PostgreSqlMaintenanceOptions { DeadRetention = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
        configuration["BatchSize"] = "broken";
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlMaintenance("bad", source, configuration)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Connected_sweeper_holds_leadership_through_native_send_then_rival_takes_over_after_release()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        var settings = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { Pooling = true, MaxPoolSize = 2 };
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstTransport = new RecordingTransport(async (_, token) => { entered.TrySetResult(); await release.Task.WaitAsync(token); });
        var secondTransport = new RecordingTransport();
        using var first = Worker(source, firstTransport);
        using var second = Worker(source, secondTransport);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var firstPass = first.SweepAsync(timeout.Token).AsTask();
        await entered.Task.WaitAsync(timeout.Token);
        var secondPass = second.SweepAsync(timeout.Token).AsTask();
        try
        {
            await using var observer = NpgsqlDataSource.Create(fixture.ConnectionString);
            while (true)
            {
                await using var waiting = observer.CreateCommand("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO %leases%'");
                if ((long)(await waiting.ExecuteScalarAsync(timeout.Token))! > 0) break;
                await Task.Yield();
            }
            await Assert.That(secondPass.IsCompleted).IsFalse();
        }
        finally { release.TrySetResult(); }
        await Assert.That(await firstPass).IsEqualTo(1);
        await Assert.That(await secondPass).IsEqualTo(0);
        await Assert.That(secondTransport.Ids.Count).IsEqualTo(0);
        await first.ReleaseAsync(timeout.Token);
        await Insert(source, Id(2));
        await Assert.That(await second.SweepAsync(timeout.Token)).IsEqualTo(1);
        await Assert.That(secondTransport.Ids.SequenceEqual([Id(2)])).IsTrue();
    }

    [Test]
    public async Task Killed_process_replays_accepted_identity_and_eventually_delivers_every_committed_message()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Sql(source, "CREATE TABLE crash_deliveries (id uuid NOT NULL, body bytea NOT NULL)");
        for (var i = 1; i <= 3; i++) await Insert(source, Id(i));
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        start.ArgumentList.Add("exec");
        start.ArgumentList.Add("--runtimeconfig");
        start.ArgumentList.Add(Path.ChangeExtension(typeof(MaintenanceTests).Assembly.Location, ".runtimeconfig.json"));
        start.ArgumentList.Add("--depsfile");
        start.ArgumentList.Add(Path.ChangeExtension(typeof(MaintenanceTests).Assembly.Location, ".deps.json"));
        start.ArgumentList.Add(typeof(CrashProbeMarker).Assembly.Location);
        start.ArgumentList.Add(fixture.ConnectionString);
        using var child = Process.Start(start)!;
        var errors = child.StandardError.ReadToEndAsync();
        try
        {
            var signal = await child.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(30));
            if (signal != "BROKER_ACCEPTED")
            {
                if (!child.HasExited) { child.Kill(entireProcessTree: true); await child.WaitForExitAsync(); }
                throw new InvalidOperationException($"Crash probe did not reach send: {signal}; {await errors}");
            }
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync();
        }
        finally
        {
            if (!child.HasExited) { child.Kill(entireProcessTree: true); await child.WaitForExitAsync(); }
        }
        // Advance durable expiry explicitly rather than relying on a timing race or sleeping.
        await Sql(source, "UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE claim_token IS NOT NULL; UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second'");
        var transport = new RecordingTransport(async (message, token) =>
        {
            await using var delivered = source.CreateCommand("INSERT INTO crash_deliveries (id, body) VALUES (@id, @body)");
            delivered.Parameters.AddWithValue("id", HeaderId(message));
            delivered.Parameters.AddWithValue("body", message.Body.ToArray());
            await delivered.ExecuteNonQueryAsync(token);
        });
        using var survivor = Worker(source, transport, new() { BatchSize = 1 });
        for (var i = 0; i < 3; i++) await survivor.SweepAsync(default);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
        await using var result = source.CreateCommand("SELECT count(*), count(DISTINCT id), bool_and(body = decode('070809', 'hex')) FROM crash_deliveries");
        await using var reader = await result.ExecuteReaderAsync();
        await reader.ReadAsync();
        await Assert.That(reader.GetInt64(0)).IsEqualTo(4L);
        await Assert.That(reader.GetInt64(1)).IsEqualTo(3L);
        await Assert.That(reader.GetBoolean(2)).IsTrue();
    }

    [Test]
    public async Task Recovery_uses_native_batch_limits_and_quotes_custom_tables()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        var settings = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { Pooling = true, MaxPoolSize = 1 };
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        var options = new PostgreSqlMaintenanceOptions { Schema = "Order\";--", TablePrefix = "a.\"_", RecoveryDelay = TimeSpan.Zero, BatchSize = 3 };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options.Validate());
        var table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema!) + "." + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        for (var i = 1; i <= 3; i++)
        {
            var headers = new ArrayBufferWriter<byte>();
            OutboxHeaderCodec.Write(headers, [new("ce_id", new(Id(i).ToString()))]);
            await using var insert = source.CreateCommand($"INSERT INTO {table} (id, transport, destination, message_type, headers, body, created_at) VALUES (@id, 'events', 'orders', 'order;v=1', @headers, @body, clock_timestamp())");
            insert.Parameters.AddWithValue("id", Id(i));
            insert.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
            insert.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
            await insert.ExecuteNonQueryAsync();
        }
        var transport = new RecordingTransport { MaxBatchSize = 2 };
        using var worker = Worker(source, transport, options);
        await Assert.That(await worker.SweepAsync(default)).IsEqualTo(3);
        await Assert.That(transport.BatchSizes.SequenceEqual([2])).IsTrue();
        await Assert.That(transport.Ids.Count).IsEqualTo(3);
        await using var remaining = source.CreateCommand($"SELECT count(*) FROM {table}");
        await Assert.That((long)(await remaining.ExecuteScalarAsync())!).IsEqualTo(0L);
        await Assert.That(await worker.CleanupAsync(default)).IsEqualTo(0);
        await worker.ReleaseAsync(default);
    }

    [Test]
    public async Task Hosted_shutdown_cancels_send_rolls_back_claim_and_releases_only_owned_leases()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        await Insert(source, Id(1));
        await Sql(source, "INSERT INTO relayer_leases VALUES ('unrelated', 'other-owner', clock_timestamp() + interval '1 day')");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new RecordingTransport(async (_, token) =>
        {
            entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        });
        using var worker = Worker(source, transport);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StartAsync(timeout.Token);
        try { await entered.Task.WaitAsync(timeout.Token); }
        finally { await worker.StopAsync(timeout.Token); }
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(1L);
        await Assert.That(await Count(source, "relayer_leases")).IsEqualTo(1L);
        await using var row = source.CreateCommand("SELECT attempts, claim_token IS NOT NULL FROM relayer_outbox");
        await using var reader = await row.ExecuteReaderAsync();
        await reader.ReadAsync();
        await Assert.That(reader.GetInt32(0)).IsEqualTo(0);
        await Assert.That(reader.GetBoolean(1)).IsFalse();
    }

    private static PostgreSqlMaintenanceWorker Worker(NpgsqlDataSource source, ITransport transport, PostgreSqlMaintenanceOptions? options = null) =>
        new(source, options ?? new(), [transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
    private static Guid Id(int i) => Guid.Parse($"01900000-0000-7000-8000-{i:D12}");
    private static Guid HeaderId(OutgoingMessage message) => Guid.Parse(message.Headers.Span[0].Value.ToString());
    private static async Task Insert(NpgsqlDataSource source, Guid id)
    {
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, [new("ce_id", new(id.ToString()))]);
        await using var command = source.CreateCommand("INSERT INTO relayer_outbox (id, transport, destination, message_type, headers, body, created_at) VALUES (@id, 'events', 'orders', 'order.placed;v=1', @headers, @body, clock_timestamp() - interval '1 day')");
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
        command.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
        await command.ExecuteNonQueryAsync();
    }
    private static async Task Sql(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }
    private static async Task<long> Count(NpgsqlDataSource source, string table)
    {
        await using var command = source.CreateCommand($"SELECT count(*) FROM {table}");
        return (long)(await command.ExecuteScalarAsync())!;
    }
    private sealed class RecordingTransport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        public string Name => "events";
        internal ConcurrentQueue<Guid> Ids { get; } = new();
        internal int MaxBatchSize { get; init; } = 1;
        internal ConcurrentQueue<int> BatchSizes { get; } = new();
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = MaxBatchSize };
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null) await send(message, cancellationToken);
            Ids.Enqueue(HeaderId(message));
        }
        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            BatchSizes.Enqueue(messages.Length);
            for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], cancellationToken);
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
