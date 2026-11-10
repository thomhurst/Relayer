using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Tool;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class SentHistoryTests
{
    [Test]
    [Arguments("direct")]
    [Arguments("ordered")]
    [Arguments("maintenance")]
    [Arguments("cdc")]
    [Arguments("tenant")]
    public async Task Startup_rejects_missing_history_table_before_dispatch(string mode)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "old\"schema", TablePrefix = "test_", RetainSentMessages = true };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        await Sql(source, "DROP TABLE \"old\"\"schema\".test_outbox_sent");
        var transport = new Transport(1);
        using Microsoft.Extensions.Hosting.BackgroundService worker = mode switch
        {
            "direct" => new PostgreSqlDirectDispatcher(source, options, new(), [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance),
            "ordered" => new PostgreSqlOrderedDispatcher(source, new() { Schema = options.Schema, TablePrefix = options.TablePrefix, RetainSentMessages = true },
                [transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance),
            "maintenance" => new PostgreSqlMaintenanceWorker(source, new() { Schema = options.Schema, TablePrefix = options.TablePrefix, RetainSentMessages = true },
                [transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance),
            "cdc" => new PostgreSqlChangeCaptureWorker(source, () => throw new InvalidOperationException("Must not open replication"),
                new() { Schema = options.Schema, TablePrefix = options.TablePrefix, RetainSentMessages = true,
                    Slot = new("unused"), Replication = new("unused", Npgsql.Replication.PgOutput.PgOutputProtocolVersion.V1) },
                [transport], NullLogger<PostgreSqlChangeCaptureWorker>.Instance, NullLogger<PostgreSqlMaintenanceWorker>.Instance,
                NullLogger<PostgreSqlDirectDispatcher>.Instance),
            _ => new PostgreSqlTenantDispatcher(new HistoryCatalog(new PostgreSqlTenantStore("tenant", source, options)), new(), [transport], NullLoggerFactory.Instance)
        };
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => worker.StartAsync(default));
        await Assert.That(error!.Message).Contains("test_outbox_sent");
        await Assert.That(error.Message).Contains("PostgreSqlOutboxSchema.CreateScript");
        await Assert.That(transport.Count).IsEqualTo(0);
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        await PostgreSqlSentSchema.ValidateAsync(source, true, options.Schema, options.TablePrefix, default);
        await Sql(source, "DROP TABLE \"old\"\"schema\".test_outbox_sent");
        await PostgreSqlSentSchema.ValidateAsync(source, false, options.Schema, options.TablePrefix, default);
    }

    [Test]
    public async Task New_catalog_entry_cannot_send_until_history_schema_is_deployed()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { RetainSentMessages = true };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        await Sql(source, "DROP TABLE relayer_outbox_sent");
        var catalog = new HistoryCatalog();
        var transport = new Transport(1);
        using var worker = new PostgreSqlTenantDispatcher(catalog, new(), [transport], NullLoggerFactory.Instance);
        await worker.PassAsync(default);
        catalog.Entries = [new("tenant", source, options)];
        var id = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, now);
        var row = new BufferedOutboxMessage();
        row.Copy(new(id, "events", "order.placed.v1", new("orders", new byte[] { 1 }), now), 32);
        worker.GetDispatcher("tenant").Committed([row], 1);
        await worker.PassAsync(default);
        await Assert.That(transport.Count).IsEqualTo(0);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(1L);
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        await worker.PassAsync(default);
        await Assert.That(transport.Count).IsEqualTo(1);
        await Assert.That(await Count(source, "relayer_outbox_sent")).IsEqualTo(1L);
        await worker.ReleaseAllAsync();
    }

    private sealed class HistoryCatalog(params PostgreSqlTenantStore[] entries) : ITenantSource
    {
        internal IReadOnlyList<PostgreSqlTenantStore> Entries { get; set; } = entries;
        public IReadOnlyList<PostgreSqlTenantStore> GetTenants() => Entries;
    }

    [Test]
    public async Task Completion_retains_only_live_fenced_rows_and_rolls_back_if_history_write_fails()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "sent\"schema", TablePrefix = "test_", RetainSentMessages = true };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        var table = "\"sent\"\"schema\".test_outbox";
        var history = "\"sent\"\"schema\".test_outbox_sent";
        var store = new PostgreSqlOutboxStore(source, options);
        var now = new DateTimeOffset(2026, 11, 10, 12, 0, 0, TimeSpan.Zero);
        var live = new OutboxClaim(Guid.NewGuid(), Guid.NewGuid());
        var expired = new OutboxClaim(Guid.NewGuid(), Guid.NewGuid());
        var replaced = new OutboxClaim(Guid.NewGuid(), Guid.NewGuid());
        foreach (var claim in new[] { live, expired, replaced })
        {
            await PostgreSqlOutboxStoreTests.Insert(source, options, claim.MessageId, now);
            await store.TryClaimAsync(claim, now, now.AddMinutes(1), default);
        }
        await Assert.That(await store.CompleteAsync(new(live.MessageId, Guid.NewGuid()), now, default)).IsFalse();
        await Assert.That(await store.CompleteAsync(expired, now.AddMinutes(1), default)).IsFalse();
        await Assert.That(await store.CompleteBatchAsync(new OutboxClaim[] { expired }, now.AddMinutes(1), default)).IsEqualTo(0L);
        await Assert.That(await store.CompleteBatchAsync(new OutboxClaim[] { live, live, new(replaced.MessageId, Guid.NewGuid()), new(Guid.NewGuid(), Guid.NewGuid()) }, now, default)).IsEqualTo(1L);
        await Assert.That(await Count(source, history)).IsEqualTo(1L);
        var location = await new PostgreSqlMessageLookup(source, options).FindAsync(live.MessageId);
        await Assert.That(location.State).IsEqualTo("sent");
        await Assert.That(location.Destination).IsEqualTo("orders");
        await Assert.That(location.SentAt).IsEqualTo(now);
        await Assert.That(await store.CompleteAsync(replaced, now, default)).IsTrue();
        await Assert.That(await store.CompleteAsync(replaced, now, default)).IsFalse();
        await Sql(source, $"ALTER TABLE {history} ADD CONSTRAINT reject_history CHECK (false) NOT VALID");
        await Assert.ThrowsAsync<PostgresException>(async () => await store.CompleteAsync(expired, now, default));
        await Assert.That(await Count(source, table)).IsEqualTo(1L);
        await Assert.That(await Count(source, history)).IsEqualTo(2L);
        await Sql(source, $"ALTER TABLE {history} DROP CONSTRAINT reject_history");
        // Default completion neither needs nor touches the sent table.
        await Sql(source, $"DROP TABLE {history}");
        var disabled = new PostgreSqlOutboxStore(source, new() { Schema = options.Schema, TablePrefix = options.TablePrefix });
        await Assert.That(await disabled.CompleteAsync(expired, now, default)).IsTrue();
    }

    [Test]
    [Arguments(false, 1)]
    [Arguments(true, 1)]
    [Arguments(true, 3)]
    public async Task Direct_and_recovery_dispatch_retain_metadata_after_native_send(bool retain, int batchSize)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { RetainSentMessages = retain };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        var now = DateTimeOffset.UtcNow;
        var transport = new Transport(batchSize);
        var logger = new DirectDispatchTests.CaptureLogger();
        var rows = new List<BufferedOutboxMessage>();
        for (var i = 0; i < 3; i++)
        {
            var id = Guid.CreateVersion7();
            await PostgreSqlOutboxStoreTests.Insert(source, options, id, now.AddDays(-1));
            var row = new BufferedOutboxMessage();
            row.Copy(new(id, "events", "order.placed.v1", new("orders", new byte[] { 1 }), now), 32);
            rows.Add(row);
        }
        using (var direct = new PostgreSqlDirectDispatcher(source, options, new() { BatchSize = 3 }, [transport], logger))
        {
            direct.Committed(rows, rows.Count);
            await direct.DispatchQueuedAsync(default);
        }
        await Assert.That(transport.Count).IsEqualTo(3);
        await Assert.That(await Count(source, "relayer_outbox_sent")).IsEqualTo(retain ? 3L : 0L);
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
        for (var i = 0; i < 3; i++)
            await PostgreSqlOutboxStoreTests.Insert(source, options, Guid.CreateVersion7(), now.AddDays(-1));
        using var recovery = new PostgreSqlMaintenanceWorker(source, new() { RetainSentMessages = retain, RecoveryDelay = TimeSpan.Zero },
            [transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance, logger);
        // Unordered writer rows have no ordering key; the shared store helper deliberately supplies one.
        await Sql(source, "UPDATE relayer_outbox SET ordering_key = NULL WHERE lane IS NULL");
        await Assert.That(await recovery.SweepAsync(default)).IsEqualTo(3);
        await Assert.That(await Count(source, "relayer_outbox_sent")).IsEqualTo(retain ? 6L : 0L);
        await Assert.That(transport.Count).IsEqualTo(6);
        await Assert.That(transport.Batches).IsEqualTo(batchSize == 3 ? 2 : 0);
    }

    [Test]
    public async Task Ordered_completion_is_fenced_and_transactional()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { RetainSentMessages = true };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        var id = Guid.NewGuid();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow, lane: 0);
        using var lane = new PostgreSqlOrderedLane(source, options, TimeSpan.FromMinutes(1));
        await using var connection = await source.OpenConnectionAsync();
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            var claim = new OutboxClaim(id, Guid.NewGuid());
            await Assert.That(await lane.ClaimAsync(connection, transaction, claim, default)).IsTrue();
            await Assert.That(await lane.CompleteAsync(connection, transaction, new(id, Guid.NewGuid()), default)).IsFalse();
            await using var expire = new NpgsqlCommand("UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() - interval '1 minute'", connection, transaction);
            await expire.ExecuteNonQueryAsync();
            await Assert.That(await lane.CompleteAsync(connection, transaction, claim, default)).IsFalse();
            await transaction.RollbackAsync();
        }
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            var claim = new OutboxClaim(id, Guid.NewGuid());
            await lane.ClaimAsync(connection, transaction, claim, default);
            await Assert.That(await lane.CompleteAsync(connection, transaction, claim, default)).IsTrue();
            await transaction.RollbackAsync();
        }
        await Assert.That(await Count(source, "relayer_outbox_sent")).IsEqualTo(0L);
        using var ordered = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1, RetainSentMessages = true },
            [new Transport(1)], NullLogger<PostgreSqlOrderedDispatcher>.Instance);
        await ordered.DispatchPassAsync(TimeSpan.MaxValue, default);
        await Assert.That((await new PostgreSqlMessageLookup(source, new() { RetainSentMessages = true }).FindAsync(id)).State).IsEqualTo("sent");
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(0L);
    }

    [Test]
    public async Task Leased_cleanup_is_bounded_and_lookup_cli_reports_sent_then_absent()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var id = Guid.NewGuid();
        await using (var insert = source.CreateCommand("INSERT INTO relayer_outbox_sent VALUES (@id, 'orders', clock_timestamp()), (gen_random_uuid(), 'old', clock_timestamp() - interval '10 days'), (gen_random_uuid(), 'old', clock_timestamp() - interval '10 days')"))
        { insert.Parameters.AddWithValue("id", id); await insert.ExecuteNonQueryAsync(); }
        await using (var columns = source.CreateCommand("SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'relayer_outbox_sent'"))
            await Assert.That((string)(await columns.ExecuteScalarAsync())!).IsEqualTo("id,destination,sent_at");
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(typeof(RelayerCommand).Assembly.Location);
        foreach (var argument in new[] { "message", "locate", "--sent-history", "--id", id.ToString() }) start.ArgumentList.Add(argument);
        start.Environment["RELAYER_POSTGRES_CONNECTION_STRING"] = fixture.ConnectionString;
        using (var process = Process.Start(start)!)
        {
            var output = process.StandardOutput.ReadToEndAsync(); var error = process.StandardError.ReadToEndAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try { await process.WaitForExitAsync(timeout.Token); }
            finally { if (!process.HasExited) process.Kill(entireProcessTree: true); }
            await Assert.That(process.ExitCode).IsEqualTo(0);
            await Assert.That(await output).Contains("State: sent\nDestination: orders");
            await Assert.That(await output).Contains("Sent: ");
            await Assert.That(await error).IsEqualTo("");
        }
        await Assert.That((await new PostgreSqlMessageLookup(source).FindAsync(id)).State).IsEqualTo("not present");
        var settings = new PostgreSqlMaintenanceOptions { RetainSentMessages = true, BatchSize = 1, DeadRetention = null, InboxRetention = null };
        using var first = new PostgreSqlMaintenanceWorker(source, settings, [], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        using var rival = new PostgreSqlMaintenanceWorker(source, settings, [], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        await Assert.That(await first.CleanupAsync(default)).IsEqualTo(1);
        await Assert.That(await rival.CleanupAsync(default)).IsEqualTo(0);
        await first.ReleaseAsync(default);
        await Assert.That(await rival.CleanupAsync(default)).IsEqualTo(1);
        await Assert.That(await Count(source, "relayer_outbox_sent")).IsEqualTo(1L);
        await Sql(source, "UPDATE relayer_outbox_sent SET sent_at = clock_timestamp() - interval '10 days'");
        await Assert.That(await rival.CleanupAsync(default)).IsEqualTo(1);
        await Assert.That((await new PostgreSqlMessageLookup(source, new() { RetainSentMessages = true }).FindAsync(id)).State).IsEqualTo("not present");
    }

    [Test]
    public async Task Cleanup_preserves_history_refreshed_while_waiting_for_completion()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { RetainSentMessages = true };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        var id = Guid.NewGuid();
        await using (var insert = source.CreateCommand("INSERT INTO relayer_outbox_sent VALUES (@id, 'old', clock_timestamp() - interval '10 days')"))
        { insert.Parameters.AddWithValue("id", id); await insert.ExecuteNonQueryAsync(); }
        var clock = DateTimeOffset.UtcNow;
        // PostgreSQL timestamps retain microseconds, while Linux UtcNow can include 100 ns ticks.
        var now = clock.AddTicks(-(clock.Ticks % 10));
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, now);
        var store = new PostgreSqlOutboxStore(source, options);
        var claim = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await using var completion = await source.OpenConnectionAsync();
        await using var transaction = await completion.BeginTransactionAsync();
        store.UseDispatchTransaction(completion, transaction);
        await Assert.That(await store.CompleteAsync(claim, now, default)).IsTrue();
        using var worker = new PostgreSqlMaintenanceWorker(source,
            new() { RetainSentMessages = true, DeadRetention = null, InboxRetention = null },
            [], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var cleanup = worker.CleanupAsync(timeout.Token).AsTask();
        var committed = false;
        try
        {
            // Wait for the DELETE's actual lock wait, rather than guessing when its snapshot was taken.
            await using var blocked = source.CreateCommand("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE @pid = ANY(pg_blocking_pids(pid)))");
            blocked.Parameters.AddWithValue("pid", completion.ProcessID);
            while (await blocked.ExecuteScalarAsync(timeout.Token) is not true)
                await Task.Delay(10, timeout.Token);
            await transaction.CommitAsync(timeout.Token);
            committed = true;
            await Assert.That(await cleanup).IsEqualTo(0);
            var location = await new PostgreSqlMessageLookup(source, options).FindAsync(id);
            await Assert.That(location.State).IsEqualTo("sent");
            await Assert.That(location.SentAt).IsEqualTo(now);
        }
        finally
        {
            // Release the row lock even if observing the wait fails; never strand the cleanup task.
            if (!committed) await transaction.RollbackAsync();
            try { await cleanup; }
            finally { await worker.ReleaseAsync(default); }
        }
    }

    [Test]
    public async Task Configuration_preserves_opt_in_and_requires_finite_positive_retention()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        { ["RetainSentMessages"] = "true", ["SentRetention"] = "2.00:00:00" }).Build();
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerPostgreSqlMaintenance("test", source, configuration);
        services.AddRelayerPostgreSqlOrderedDispatch("test", source, configuration);
        services.AddRelayerPostgreSqlOutbox(configuration);
        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get("test").SentRetention).IsEqualTo(TimeSpan.FromDays(2));
        await Assert.That(provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get("test").Validate().RetainSentMessages).IsTrue();
        await Assert.That(provider.GetRequiredService<IOptionsMonitor<PostgreSqlOrderedDispatchOptions>>().Get("test").Validate().RetainSentMessages).IsTrue();
        await Assert.That(provider.GetRequiredService<IOptionsMonitor<PostgreSqlOutboxOptions>>().CurrentValue.RetainSentMessages).IsTrue();
        await Assert.That(() => new PostgreSqlMaintenanceOptions { SentRetention = TimeSpan.Zero }.Validate()).Throws<ArgumentOutOfRangeException>();
    }

    private static async Task Sql(NpgsqlDataSource source, string sql)
    { await using var command = source.CreateCommand(sql); await command.ExecuteNonQueryAsync(); }
    private static async Task<long> Count(NpgsqlDataSource source, string table)
    { await using var command = source.CreateCommand($"SELECT count(*) FROM {table}"); return (long)(await command.ExecuteScalarAsync())!; }

    private sealed class Transport(int batchSize) : ITransport
    {
        public string Name => "events";
        internal int Count { get; private set; }
        internal int Batches { get; private set; }
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = batchSize, OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Count++; return ValueTask.CompletedTask; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) { Batches++; Count += messages.Length; return ValueTask.CompletedTask; }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
