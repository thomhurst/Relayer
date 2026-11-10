using System.Buffers;
using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class TenantDispatchTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Catalog_recovers_ordered_and_unordered_rows_with_isolated_leases_and_cleanup(bool databases)
    {
        await using var fixture = new Stores();
        await fixture.Start(2, databases);
        var transport = new Transport();
        using var worker = Worker(fixture.Catalog, transport);
        foreach (var store in fixture.Entries)
        {
            await Insert(store, 2, 0);
            await Insert(store, 1, 0); // Same key: insertion order must win over UUID order.
            await Insert(store, 3, null);
            await Sql(store, $"INSERT INTO {Table(store, "inbox")} (consumer,message_id,processed_at) VALUES ('consumer', '{Id(1)}', clock_timestamp() - interval '1 day')");
        }
        await worker.PassAsync(default);
        foreach (var store in fixture.Entries)
        {
            var sent = transport.Sent.Where(x => x.Tenant == store.TenantId).ToArray();
            await Assert.That(sent.Select(x => x.Id).SequenceEqual([Id(3), Id(2), Id(1)])).IsTrue();
            await Assert.That(await Count(store, "outbox")).IsEqualTo(0L);
            await Assert.That(await Count(store, "inbox")).IsEqualTo(0L);
            await Assert.That(await Count(store, "leases")).IsEqualTo(4L); // sweeper, cleanup, member and lane
        }
        var owners = new List<string>();
        foreach (var store in fixture.Entries)
        {
            await using var query = store.DataSource.CreateCommand($"SELECT owner FROM {Table(store, "leases")} WHERE name='sweeper'");
            owners.Add((string)(await query.ExecuteScalarAsync())!);
        }
        await Assert.That(owners.Distinct().Count()).IsEqualTo(2);
        await worker.ReleaseAllAsync();
        foreach (var store in fixture.Entries) await Assert.That(await Count(store, "leases")).IsEqualTo(0L);
    }

    [Test]
    public async Task One_budget_bounds_native_connections_and_does_not_retain_idle_pools()
    {
        await using var fixture = new Stores();
        await fixture.Start(5);
        foreach (var store in fixture.Entries) await Insert(store, 1, null);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var concurrent = 0;
        var transport = new Transport(async (_, token) =>
        {
            if (Interlocked.Increment(ref concurrent) == 2) entered.TrySetResult();
            try { await release.Task.WaitAsync(token); }
            finally { Interlocked.Decrement(ref concurrent); }
        });
        using var worker = Worker(fixture.Catalog, transport, connections: 2);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var pass = worker.PassAsync(timeout.Token);
        try
        {
            await entered.Task.WaitAsync(timeout.Token);
            await Assert.That(await fixture.Connections()).IsEqualTo(2L);
        }
        finally { release.TrySetResult(); }
        await pass;
        await Assert.That(transport.Sent.Count).IsEqualTo(5);
        await fixture.WaitForNoConnections();
        await worker.ReleaseAllAsync();
        await fixture.WaitForNoConnections();
    }

    [Test]
    public async Task Catalog_refresh_adds_removes_and_replaces_stores_without_permanent_workers()
    {
        await using var fixture = new Stores();
        await fixture.Start(3);
        var original = fixture.Entries;
        fixture.Catalog.Entries = [original[0]];
        var transport = new Transport();
        using var worker = Worker(fixture.Catalog, transport);
        await Insert(original[0], 1, null);
        await worker.PassAsync(default);
        fixture.Catalog.Entries = [original[1], original[2]];
        await Insert(original[1], 1, 0);
        await Insert(original[2], 1, null);
        await worker.PassAsync(default);
        await Assert.That(transport.Sent.Count).IsEqualTo(3);
        await Assert.That(await Count(original[0], "leases")).IsEqualTo(0L);
        // A replacement is a new physical store under the same stable dispatch name.
        fixture.Catalog.Entries = [original[0] with { TenantId = original[1].TenantId }];
        await worker.PassAsync(default);
        await Assert.That(await Count(original[1], "leases")).IsEqualTo(0L);
        await Assert.That(await Count(original[2], "leases")).IsEqualTo(0L);
        await worker.ReleaseAllAsync();
    }

    [Test]
    public async Task Failed_store_and_invalid_dynamic_entry_do_not_block_healthy_tenants_or_leak_capacity()
    {
        await using var fixture = new Stores();
        await fixture.Start(1);
        await using var unavailable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=postgres;Pooling=false;Timeout=1;SSL Mode=Disable;GSS Encryption Mode=Disable");
        await using var pooled = NpgsqlDataSource.Create("Host=unused");
        var healthy = fixture.Entries[0];
        fixture.Catalog.Entries = [new("offline", unavailable, new()), new("invalid", pooled, new()), healthy];
        var transport = new Transport();
        using var worker = Worker(fixture.Catalog, transport, connections: 1);
        await Insert(healthy, 1, null);
        await worker.PassAsync(default);
        await Assert.That(transport.Sent.Count).IsEqualTo(1);
        fixture.Catalog.Entries = [healthy];
        await worker.PassAsync(default);
        await fixture.WaitForNoConnections();
        await worker.ReleaseAllAsync();
    }

    [Test]
    public async Task Cancelled_turn_rolls_back_and_shutdown_releases_all_owned_leases()
    {
        await using var fixture = new Stores();
        await fixture.Start(2);
        foreach (var store in fixture.Entries) await Insert(store, 1, null);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = true;
        var transport = new Transport(async (_, token) =>
        {
            if (block) { entered.TrySetResult(); await Task.Delay(Timeout.Infinite, token); }
        });
        using var worker = Worker(fixture.Catalog, transport, connections: 1);
        using var cancel = new CancellationTokenSource();
        var pass = worker.PassAsync(cancel.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        cancel.Cancel();
        await Assert.That(async () => await pass).Throws<OperationCanceledException>();
        await fixture.WaitForNoConnections();
        block = false;
        await worker.StartAsync(default);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (transport.Sent.Count < 2) await Task.Delay(10, timeout.Token);
        await worker.StopAsync(timeout.Token);
        foreach (var store in fixture.Entries) await Assert.That(await Count(store, "leases")).IsEqualTo(0L);
        await fixture.WaitForNoConnections();
    }

    [Test]
    public async Task Commit_handoff_uses_catalog_dispatcher_and_preserves_stored_headers()
    {
        await using var fixture = new Stores();
        await fixture.Start(1);
        var store = fixture.Entries[0];
        await Insert(store, 1, null);
        var transport = new Transport();
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITenantSource>(fixture.Catalog);
        services.AddSingleton<ITransport>(transport);
        services.AddRelayerPostgreSqlTenantDispatch(options: new() { RecoveryDelay = TimeSpan.FromDays(1) });
        services.AddRelayerEntityFrameworkCoreOutbox();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var builder = new DbContextOptionsBuilder().UseNpgsql(store.DataSource).AddRelayerOutbox(scope.ServiceProvider,
            new EntityFrameworkCoreOutboxOptions { DispatchStoreName = store.TenantId, Schema = store.Options.Schema, Lanes = 1 });
        await using var db = new DbContext(builder.Options);
        var dispatcher = provider.GetRequiredService<PostgreSqlTenantDispatcher>();
        var reader = new PostgreSqlOutboxStore(store.DataSource, store.Options);
        var records = new OutboxRecord[1];
        await reader.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), records, default);
        await Sql(store, $"DELETE FROM {Table(store, "outbox")}");
        using (db.EnlistRelayerOutbox())
        {
            var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
            await Assert.That(enlistment.IsActive).IsTrue();
            await Assert.That(enlistment.SupportsScheduling).IsTrue();
            await Assert.That(enlistment.SupportsOrderedScheduling).IsTrue();
            await enlistment.EnlistAsync(records[0], default);
            await db.SaveChangesAsync();
        }
        await dispatcher.PassAsync(default);
        await Assert.That(transport.Sent.Single()).IsEqualTo((store.TenantId, Id(1)));
        await Assert.That(await Count(store, "outbox")).IsEqualTo(0L);
        await dispatcher.ReleaseAllAsync();
    }

    [Test]
    public async Task Contended_native_lease_does_not_duplicate_or_cross_tenant_dispatch()
    {
        await using var fixture = new Stores();
        await fixture.Start(1);
        await Insert(fixture.Entries[0], 1, null);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstTransport = new Transport(async (_, token) => { entered.TrySetResult(); await release.Task.WaitAsync(token); });
        var rivalTransport = new Transport();
        using var first = Worker(fixture.Catalog, firstTransport, connections: 1);
        using var rival = Worker(fixture.Catalog, rivalTransport, connections: 1);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var firstPass = first.PassAsync(timeout.Token);
        await entered.Task.WaitAsync(timeout.Token);
        var rivalPass = rival.PassAsync(timeout.Token);
        try
        {
            while (true)
            {
                await using var observer = fixture.Entries[0].DataSource.CreateCommand($"SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'INSERT INTO %leases%' AND application_name='{fixture.Application}'");
                if ((long)(await observer.ExecuteScalarAsync(timeout.Token))! > 0) break;
                await Task.Yield();
            }
            await Assert.That(rivalPass.IsCompleted).IsFalse();
        }
        finally { release.TrySetResult(); }
        await Task.WhenAll(firstPass, rivalPass);
        await Assert.That(firstTransport.Sent.Count).IsEqualTo(1);
        await Assert.That(rivalTransport.Sent.Count).IsEqualTo(0);
        await first.ReleaseAllAsync();
        await rival.ReleaseAllAsync();
    }

    [Test]
    public async Task Turn_deadline_releases_admission_for_an_unrelated_tenant()
    {
        await using var fixture = new Stores();
        await fixture.Start(2);
        foreach (var store in fixture.Entries) await Insert(store, 1, null);
        var entered = false;
        var transport = new Transport(async (message, token) =>
        {
            var blocked = false;
            foreach (var header in message.Headers.Span)
                if (header.Key == "ce_tenant" && header.Value.ToString() == fixture.Entries[0].TenantId) blocked = true;
            if (blocked) { entered = true; await Task.Delay(Timeout.Infinite, token); }
        });
        using var worker = new PostgreSqlTenantDispatcher(fixture.Catalog,
            new() { MaxConnections = 1, TurnTimeout = TimeSpan.FromSeconds(2), RecoveryDelay = TimeSpan.Zero },
            [transport], NullLoggerFactory.Instance);
        await worker.PassAsync(default);
        await Assert.That(entered).IsTrue();
        await Assert.That(transport.Sent.Single().Tenant).IsEqualTo(fixture.Entries[1].TenantId);
        await Assert.That(await Count(fixture.Entries[0], "outbox")).IsEqualTo(1L);
        await fixture.WaitForNoConnections();
        await worker.ReleaseAllAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Ordered_lanes_progress_after_lane_or_tenant_deadline(bool tenantDeadline)
    {
        await using var fixture = new Stores();
        // Wall-clock deadlines: the lane deadline is half the lease. Leave room for a busy shared server.
        await fixture.Start(1, lanes: 3, leaseDuration: TimeSpan.FromSeconds(12));
        var store = fixture.Entries[0];
        for (var lane = 0; lane < 3; lane++) await Insert(store, lane + 1, lane);
        var blockedLaneEntered = false;
        var transport = new Transport(async (message, token) =>
        {
            if (MessageId(message) == Id(1))
            {
                blockedLaneEntered = true;
                await Task.Delay(Timeout.Infinite, token);
            }
        });
        using var worker = new PostgreSqlTenantDispatcher(fixture.Catalog,
            new() { TurnTimeout = TimeSpan.FromSeconds(tenantDeadline ? 3 : 15), RecoveryDelay = TimeSpan.Zero },
            [transport], NullLoggerFactory.Instance);
        // A turn includes schema validation, recovery and real SQL work. A busy
        // server can exhaust it before reaching every ready lane. Assert eventual
        // cursor progress after the blocked lane expires, not an exact pass count.
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        do
        {
            await worker.PassAsync(guard.Token);
        } while (!blockedLaneEntered || transport.Sent.Count < 2);
        await Assert.That(transport.Sent.Select(x => x.Id).SequenceEqual([Id(2), Id(3)])).IsTrue();
        await Assert.That(await Count(store, "outbox")).IsEqualTo(1L);
        await fixture.WaitForNoConnections();
        await worker.ReleaseAllAsync();
    }

    [Test]
    public async Task Ordered_batches_commit_progress_within_a_short_tenant_turn()
    {
        await using var fixture = new Stores();
        await fixture.Start(1);
        var store = fixture.Entries[0];
        for (var number = 1; number <= 3; number++) await Insert(store, number, 0);
        var clock = new AdmissionClock();
        var transport = new Transport((_, _) =>
        {
            // Advance beyond half the ten-second tenant turn, but below the default
            // lane's 7.5-second admission budget. The first row must commit before
            // another row is admitted, without sleeping or racing real SQL work.
            clock.Advance(TimeSpan.FromSeconds(6));
            return default;
        });
        using var worker = new PostgreSqlTenantDispatcher(fixture.Catalog,
            new() { TurnTimeout = TimeSpan.FromSeconds(10), RecoveryDelay = TimeSpan.Zero },
            [transport], NullLoggerFactory.Instance, orderedTimeProvider: clock);
        for (var remaining = 2; remaining >= 0; remaining--)
        {
            await worker.PassAsync(default);
            await Assert.That(await Count(store, "outbox")).IsEqualTo((long)remaining);
        }
        await Assert.That(transport.Sent.Select(x => x.Id).SequenceEqual([Id(1), Id(2), Id(3)])).IsTrue();
        await fixture.WaitForNoConnections();
        await worker.ReleaseAllAsync();
    }

    private sealed class AdmissionClock : TimeProvider
    {
        private long _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _timestamp;
        internal void Advance(TimeSpan elapsed) => _timestamp += elapsed.Ticks;
    }

    [Test]
    public async Task Recovery_groups_commit_progress_within_a_short_tenant_turn()
    {
        await using var fixture = new Stores();
        await fixture.Start(1);
        var store = fixture.Entries[0];
        for (var number = 1; number <= 3; number++)
        {
            await Insert(store, number, null);
            await Sql(store, $"UPDATE {Table(store, "outbox")} SET destination = 'destination-{number}' WHERE id = '{Id(number)}'");
        }
        var clock = new AdmissionClock();
        var transport = new Transport((_, _) =>
        {
            // Exhaust admission after one destination without consuming the real
            // tenant deadline needed for SQL setup and transaction commit.
            clock.Advance(TimeSpan.FromMinutes(1));
            return default;
        });
        using var worker = new PostgreSqlTenantDispatcher(fixture.Catalog,
            new() { TurnTimeout = TimeSpan.FromSeconds(30), RecoveryDelay = TimeSpan.Zero },
            [transport], NullLoggerFactory.Instance, recoveryTimeProvider: clock);
        await worker.PassAsync(default);
        await Assert.That(await Count(store, "outbox")).IsEqualTo(2L);
        // Each remaining group needs one pass; an empty boundary pass wraps the UUID cursor.
        for (var turn = 0; turn < 4; turn++)
            await worker.PassAsync(default);
        await Assert.That(await Count(store, "outbox")).IsEqualTo(0L);
        await Assert.That(transport.Sent.Select(x => x.Id).Order().SequenceEqual(new[] { Id(1), Id(2), Id(3) }.Order())).IsTrue();
        await worker.ReleaseAllAsync();
    }

    [Test]
    public async Task Exhausted_direct_turn_resumes_recovery_ordered_dispatch_and_cleanup()
    {
        await using var fixture = new Stores();
        await fixture.Start(1);
        var store = fixture.Entries[0];
        await Insert(store, 1, null);
        await Insert(store, 2, null);
        await Insert(store, 3, 0);
        await Sql(store, $"INSERT INTO {Table(store, "inbox")} (consumer,message_id,processed_at) VALUES ('consumer','{Id(1)}',clock_timestamp() - interval '1 day')");
        var rows = new OutboxRecord[3];
        var reader = new PostgreSqlOutboxStore(store.DataSource, store.Options);
        await reader.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        var queued = new List<BufferedOutboxMessage> { new() };
        queued[0].Copy(rows.Single(x => x.Id == Id(1)), 32);
        var transport = new Transport(async (message, token) =>
        {
            if (MessageId(message) == Id(1)) await Task.Delay(Timeout.Infinite, token);
        });
        using var worker = new PostgreSqlTenantDispatcher(fixture.Catalog,
            new() { TurnTimeout = TimeSpan.FromSeconds(3), RecoveryDelay = TimeSpan.Zero, InboxRetention = TimeSpan.FromHours(1), BatchSize = 1 },
            [transport], NullLoggerFactory.Instance);
        worker.GetDispatcher(store.TenantId).Committed(queued, 1);
        // Keep the direct queue busy on every turn, including after canceled durable claims.
        for (var turn = 0; turn < 4; turn++)
        {
            await worker.PassAsync(default);
            if (turn < 3)
            {
                var next = new List<BufferedOutboxMessage> { new() };
                next[0].Copy(rows.Single(x => x.Id == Id(1)), 32);
                worker.GetDispatcher(store.TenantId).Committed(next, 1);
            }
        }
        await Assert.That(transport.Sent.Any(x => x.Id == Id(2))).IsTrue();
        await Assert.That(transport.Sent.Any(x => x.Id == Id(3))).IsTrue();
        await Assert.That(await Count(store, "inbox")).IsEqualTo(0L);
        await fixture.WaitForNoConnections();
        await worker.ReleaseAllAsync();
    }

    [Test]
    public async Task Connection_release_observation_rejects_a_live_backend()
    {
        await using var fixture = new Stores();
        await fixture.Start(1);
        var connection = await fixture.Entries[0].DataSource.OpenConnectionAsync();
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            await Assert.That(async () => await fixture.WaitForNoConnections(timeout.Token))
                .Throws<OperationCanceledException>();
            await Assert.That(await fixture.Connections()).IsEqualTo(1L);
        }
        finally { await connection.DisposeAsync(); }
        await fixture.WaitForNoConnections();
    }

    private static Guid MessageId(OutgoingMessage message)
    {
        foreach (var header in message.Headers.Span)
            if (header.Key == "ce_id") return Guid.Parse(header.Value.ToString());
        return Guid.Empty;
    }

    [Test]
    public async Task Configuration_snapshots_native_scheduler_limits_without_reflection()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MaxConnections"] = "3", ["QueueCapacity"] = "7", ["DeadRetention"] = "infinite"
        }).Build();
        var services = new ServiceCollection().AddRelayerPostgreSqlTenantDispatch(configuration);
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<PostgreSqlTenantDispatchOptions>>().Value;
        await Assert.That(options.MaxConnections).IsEqualTo(3);
        await Assert.That(options.QueueCapacity).IsEqualTo(7);
        await Assert.That(options.DeadRetention).IsNull();
        configuration["MaxConnections"] = "9";
        await Assert.That(options.MaxConnections).IsEqualTo(3);
        configuration["MaxConnections"] = "invalid";
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlTenantDispatch(configuration)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Registration_rejects_invalid_limits_duplicate_catalog_and_pooled_sources()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var catalog = new Catalog { Entries = [new("tenant", source, new())] };
        using var worker = Worker(catalog, new Transport());
        await Assert.That(async () => await worker.StartAsync(default)).Throws<InvalidOperationException>();
        await using var unpooled = NpgsqlDataSource.Create("Host=unused;Pooling=false");
        catalog.Entries = [new("tenant", unpooled, new()), new("tenant", unpooled, new() { Schema = "other" })];
        await Assert.That(async () => await worker.PassAsync(default)).Throws<InvalidOperationException>();
        catalog.Entries = [new("a", unpooled, new()), new("b", unpooled, new())];
        await Assert.That(async () => await worker.PassAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlTenantDispatch(options: new() { MaxConnections = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlTenantDispatch(options: new() { TurnTimeout = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
        var services = new ServiceCollection().AddRelayerPostgreSqlTenantDispatch();
        await Assert.That(() => services.AddRelayerPostgreSqlTenantDispatch()).Throws<InvalidOperationException>();
    }

    private static PostgreSqlTenantDispatcher Worker(Catalog catalog, Transport transport, int connections = 2) =>
        new(catalog, new() { MaxConnections = connections, RecoveryDelay = TimeSpan.Zero,
            InboxRetention = TimeSpan.FromHours(1), PollInterval = TimeSpan.FromMilliseconds(10) }, [transport], NullLoggerFactory.Instance);
    private static Guid Id(int n) => Guid.Parse($"01900000-0000-7000-8000-{n:x12}");
    private static string Table(PostgreSqlTenantStore store, string table) => $"\"{store.Options.Schema}\".\"relayer_{table}\"";
    private static async Task Sql(PostgreSqlTenantStore store, string sql)
    {
        await using var command = store.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }
    private static async Task<long> Count(PostgreSqlTenantStore store, string table)
    {
        await using var command = store.DataSource.CreateCommand($"SELECT count(*) FROM {Table(store, table)}");
        return (long)(await command.ExecuteScalarAsync())!;
    }
    private static async Task Insert(PostgreSqlTenantStore store, int number, int? lane)
    {
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, [new("ce_id", new(Id(number).ToString())), new("ce_tenant", new(store.TenantId))]);
        await using var command = store.DataSource.CreateCommand($"INSERT INTO {Table(store, "outbox")} (id,lane,ordering_key,transport,destination,message_type,headers,body,created_at,tenant_id) VALUES (@id,@lane,@key,'events','orders','order;v=1',@headers,@body,clock_timestamp(),@tenant)");
        command.Parameters.AddWithValue("id", Id(number));
        command.Parameters.AddWithValue("lane", NpgsqlDbType.Integer, (object?)lane ?? DBNull.Value);
        command.Parameters.AddWithValue("key", NpgsqlDbType.Text, lane is null ? DBNull.Value : "same-key");
        command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
        command.Parameters.AddWithValue("body", new byte[] { 1 });
        command.Parameters.AddWithValue("tenant", store.TenantId);
        await command.ExecuteNonQueryAsync();
    }

    private sealed class Catalog : ITenantSource
    {
        internal IReadOnlyList<PostgreSqlTenantStore> Entries { get; set; } = [];
        public IReadOnlyList<PostgreSqlTenantStore> GetTenants() => Entries;
    }
    private sealed class Stores : IAsyncDisposable
    {
        private readonly PostgreSqlFixture _fixture = new();
        // The server is shared: count only this test's sessions.
        internal string Application { get; } = "tenant-dispatch-" + Guid.NewGuid().ToString("N");
        internal Catalog Catalog { get; } = new();
        internal PostgreSqlTenantStore[] Entries { get; private set; } = [];
        internal async Task Start(int count, bool databases = false, int lanes = 1, TimeSpan? leaseDuration = null)
        {
            await _fixture.StartAsync();
            Entries = new PostgreSqlTenantStore[count];
            for (var i = 0; i < count; i++)
            {
                var settings = new NpgsqlConnectionStringBuilder(_fixture.ConnectionString) { ApplicationName = Application };
                if (databases)
                {
                    await using var admin = NpgsqlDataSource.Create(_fixture.ConnectionString);
                    await using var create = admin.CreateCommand($"CREATE DATABASE {_fixture.Database}_tenant_{i}");
                    await create.ExecuteNonQueryAsync();
                    settings.Database = $"{_fixture.Database}_tenant_{i}";
                }
                var source = NpgsqlDataSource.Create(settings.ConnectionString);
                var options = new PostgreSqlOutboxOptions { Schema = $"tenant_{i}", Lanes = lanes, LeaseDuration = leaseDuration ?? TimeSpan.FromSeconds(30) };
                Entries[i] = new($"tenant-{i}", source, options);
                await Sql(Entries[i], $"CREATE SCHEMA tenant_{i}");
                await Sql(Entries[i], PostgreSqlOutboxSchema.CreateScript(options));
            }
            Catalog.Entries = Entries;
        }
        internal async Task<long> Connections()
        {
            await using var observer = NpgsqlDataSource.Create(_fixture.ConnectionString);
            await using var command = observer.CreateCommand($"SELECT count(*) FROM pg_stat_activity WHERE application_name='{Application}'");
            return (long)(await command.ExecuteScalarAsync())!;
        }
        internal async Task WaitForNoConnections(CancellationToken cancellationToken = default)
        {
            // Npgsql's unpooled close sends Terminate without awaiting backend exit.
            // Keep one non-tenant observer open so polling does not add sessions to the count.
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(15));
            await using var observer = new NpgsqlConnection(_fixture.ConnectionString);
            await observer.OpenAsync(timeout.Token);
            await using var command = new NpgsqlCommand(
                $"SELECT count(*) FROM pg_stat_activity WHERE application_name='{Application}'", observer);
            while ((long)(await command.ExecuteScalarAsync(timeout.Token))! != 0)
                await Task.Delay(10, timeout.Token);
        }
        public async ValueTask DisposeAsync()
        {
            foreach (var entry in Entries) await entry.DataSource.DisposeAsync();
            await _fixture.DisposeAsync();
        }
    }
    private sealed class Transport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        public string Name => "events";
        internal ConcurrentQueue<(string Tenant, Guid Id)> Sent { get; } = new();
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null) await send(message, cancellationToken);
            var id = Guid.Empty;
            string? tenant = null;
            foreach (var header in message.Headers.Span)
            {
                if (header.Key == "ce_id") id = Guid.Parse(header.Value.ToString());
                if (header.Key == "ce_tenant") tenant = header.Value.ToString();
            }
            Sent.Enqueue((tenant!, id));
        }
        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], cancellationToken);
        }
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
