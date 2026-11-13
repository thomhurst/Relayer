using System.Buffers;
using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;
using Relayer.Serialization;
using Relayer.Transports;
using Transport = Relayer.TestFixtures.OutboxDirectDispatchContractTests.RecordingTransport;

namespace Relayer.Outbox.PostgreSql.Tests;

public class DirectDispatchTests
{
    [Test]
    public async Task Batch_handoff_transfers_due_rows_and_owns_memory_until_release()
    {
        var source = new List<BufferedOutboxMessage> { new() };
        source[0].Copy(Record(1), 32);
        await Assert.That(source[0].OrderingKey is null).IsTrue();
        await Assert.That(source[0].Lane is null).IsTrue();
        var batch = new OutboxDispatchBatch();
        batch.Take(source, source.Count, DateTimeOffset.UtcNow);
        await Assert.That(batch.Count).IsEqualTo(1);
        source[0].Clear();
        await Assert.That(batch.Rows[0].ToMessage().Body.ToArray()).IsEquivalentTo(new byte[] { 1 });
        await Assert.That(batch.Rows[0].ToMessage().OrderingKey is null).IsTrue();
        batch.Clear();
    }

    [Test]
    public async Task Configuration_binds_named_immutable_limits_and_registration_rejects_invalid_settings()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["QueueCapacity"] = "7",
            ["BatchSize"] = "11",
            ["ClaimDuration"] = "00:00:19"
        }).Build();
        var services = new ServiceCollection();
        services.AddRelayerPostgreSqlDirectDispatch("store", source, configuration);
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<PostgreSqlDirectDispatchOptions>>().Get("store");
        await Assert.That(options.QueueCapacity).IsEqualTo(7);
        await Assert.That(options.BatchSize).IsEqualTo(11);
        await Assert.That(options.ClaimDuration).IsEqualTo(TimeSpan.FromSeconds(19));
        await Assert.That(() => services.AddRelayerPostgreSqlDirectDispatch("store", source)).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlDirectDispatch("bad", source,
            dispatchOptions: new() { QueueCapacity = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlDirectDispatch("bad", source,
            dispatchOptions: new() { BatchSize = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlDirectDispatch("bad", source,
            dispatchOptions: new() { ClaimDuration = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
        configuration["BatchSize"] = "broken";
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlDirectDispatch("bad", source, configuration))
            .Throws<OptionsValidationException>();
    }

    [Test]
    public async Task Context_registration_rejects_a_missing_dispatcher_or_mismatched_schema()
    {
        await using var source = NpgsqlDataSource.Create("Host=unused");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerEntityFrameworkCoreOutbox();
        services.AddRelayerPostgreSqlDirectDispatch("store", source);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        await Assert.That(() => new DbContextOptionsBuilder().UseNpgsql("Host=unused").AddRelayerOutbox(scope.ServiceProvider,
            new EntityFrameworkCoreOutboxOptions { DispatchStoreName = "missing" })).Throws<InvalidOperationException>();
        await Assert.That(() => new DbContextOptionsBuilder().UseNpgsql("Host=unused").AddRelayerOutbox(scope.ServiceProvider,
            new EntityFrameworkCoreOutboxOptions { DispatchStoreName = "store", Schema = "other" })).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Implicit_commit_starts_dispatch_without_waiting_for_broker_and_outlives_scope(bool synchronous)
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { Block = true };
        await using var provider = database.Services(transport);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<TestContext>();
                using var enlistment = db.EnlistRelayerOutbox();
                await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(7),
                    options => options.Headers["custom"] = "café");
                await Assert.That(transport.Entered.Task.IsCompleted).IsFalse();
                if (synchronous) db.SaveChanges(); else await db.SaveChangesAsync();
                // Save returns while the broker call is blocked. No polling timer triggers dispatch.
                await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                await Assert.That(await database.Count()).IsEqualTo(1L);
                await Assert.That(transport.Received.Count).IsEqualTo(0);
            }
            // All scoped state, including its DbContext and serializer buffers, is now disposed.
            transport.Release.TrySetResult();
            await Stop(worker);
            var sent = transport.Received.Single();
            await Assert.That(sent.Body).IsEquivalentTo(new byte[] { 7 });
            await Assert.That(sent.Headers.Single(header => header.Key == "custom").Value).IsEquivalentTo("café"u8.ToArray());
            await Assert.That(Guid.Parse(System.Text.Encoding.UTF8.GetString(sent.Headers.Single(header => header.Key == "ce_id").Value)).Version).IsEqualTo(7);
            await Assert.That(await database.Count()).IsEqualTo(0L);
        }
        finally { transport.Release.TrySetResult(); await Stop(worker); }
    }

    [Test]
    public async Task Synchronous_explicit_transactions_dispatch_only_after_commit_and_rollback_never_sends()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { Block = true };
        await using var provider = database.Services(transport);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await using var scope = provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<TestContext>();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await using (var transaction = db.Database.BeginTransaction())
            {
                await sender.SendAsync(new Probe(1));
                db.SaveChanges();
                transaction.Rollback();
            }
            await using (var transaction = db.Database.BeginTransaction())
            {
                await sender.SendAsync(new Probe(2));
                db.SaveChanges();
                await Assert.That(transport.Entered.Task.IsCompleted).IsFalse();
                await Assert.That(await database.Count()).IsEqualTo(0L);
                transaction.Commit();
            }
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await Assert.That(await database.Count()).IsEqualTo(1L);
            transport.Release.TrySetResult();
            await Stop(worker);
            await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 2 });
            await Assert.That(await database.Count()).IsEqualTo(0L);
        }
        finally { transport.Release.TrySetResult(); await Stop(worker); }
    }

    [Test]
    public async Task Pending_native_batch_keeps_original_bytes_while_later_transactions_reuse_scoped_buffers()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { Block = true };
        await using var provider = database.Services(transport);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<TestContext>();
                using var selection = db.EnlistRelayerOutbox();
                var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
                var body = new byte[] { 1, 255, 0 };
                var header = new byte[] { 0xff, 0, 0xfe };
                var first = Record(1, headers: [new("same", new WireString(header)), new("same", new WireString("second"))], body: body);
                await outbox.EnlistAsync(first, default);
                await outbox.EnlistAsync(Record(2), default);
                Array.Fill(body, (byte)42);
                Array.Fill(header, (byte)42);
                await db.SaveChangesAsync();
                await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                await outbox.EnlistAsync(Record(3), default);
                await outbox.EnlistAsync(Record(4), default);
                await db.SaveChangesAsync();
            }
            transport.Release.TrySetResult();
            await Stop(worker);
            var firstSent = transport.Received.Single(sent => sent.Body.Length == 3);
            await Assert.That(firstSent.Body).IsEquivalentTo(new byte[] { 1, 255, 0 });
            await Assert.That(firstSent.Headers.Select(header => header.Key)).IsEquivalentTo(new[] { "same", "same" });
            await Assert.That(firstSent.Headers[0].Value).IsEquivalentTo(new byte[] { 0xff, 0, 0xfe });
            await Assert.That(firstSent.Headers[1].Value).IsEquivalentTo("second"u8.ToArray());
            await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 1, 2, 3, 4 });
            await Assert.That(await database.Count()).IsEqualTo(0L);
        }
        finally { transport.Release.TrySetResult(); await Stop(worker); }
    }

    [Test]
    public async Task Native_batch_limits_and_destinations_bound_sends_and_completion_deletes_are_batched()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport();
        await using var provider = database.Services(transport, new() { BatchSize = 3 });
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<TestContext>();
                using var selection = db.EnlistRelayerOutbox();
                var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
                await outbox.EnlistAsync(Record(1, "alpha"), default);
                await outbox.EnlistAsync(Record(2, "beta"), default);
                await outbox.EnlistAsync(Record(3, "alpha"), default);
                await outbox.EnlistAsync(Record(4, "single"), default);
                await outbox.EnlistAsync(Record(5, "alpha"), default);
                await outbox.EnlistAsync(Record(6, "beta"), default);
                await outbox.EnlistAsync(Record(7, "single"), default);
                await db.SaveChangesAsync();
            }
            await Stop(worker);
            await Assert.That(transport.Calls.Select(call => $"{call.Destination}:{call.Count}")).IsEquivalentTo(new[]
            { "alpha:2", "alpha:1", "beta:2", "single:1", "single:1" });
            await Assert.That(transport.Received.Count).IsEqualTo(7);
            await Assert.That(await database.Count()).IsEqualTo(0L);
            await Assert.That(await database.Scalar("SELECT count(*) FROM delete_log")).IsEqualTo(5L);
            await Assert.That(await database.Scalar("SELECT count(*) FROM update_log")).IsEqualTo(5L);
        }
        finally { await Stop(worker); }
    }

    [Test]
    [Arguments("throw")]
    [Arguments("zero")]
    [Arguments("negative")]
    [Arguments("missing")]
    public async Task Invalid_group_does_not_claim_rows_or_prevent_other_groups_in_same_commit(string failure)
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { CapabilityFailure = failure };
        var log = new CaptureLogger();
        await using var provider = database.Services(transport, new() { BatchSize = 2 }, logger: log);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await using (var scope = provider.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<TestContext>();
                using var selection = db.EnlistRelayerOutbox();
                var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
                // Sort the failed group first and make it larger than the native/worker batch limit.
                var transportName = failure == "missing" ? "absent" : "events";
                await outbox.EnlistAsync(Record(1, "alpha", transport: transportName), default);
                await outbox.EnlistAsync(Record(2, "alpha", transport: transportName), default);
                await outbox.EnlistAsync(Record(3, "alpha", transport: transportName), default);
                await outbox.EnlistAsync(Record(4, "beta"), default);
                await outbox.EnlistAsync(Record(5, "zeta"), default);
                await db.SaveChangesAsync();
            }
            await Stop(worker);
            await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 4, 5 });
            await Assert.That(await database.Count()).IsEqualTo(3L);
            await Assert.That(await database.Scalar($"SELECT count(*) FROM {Database.Table} WHERE attempts = 0 AND last_error IS NULL AND claim_token IS NULL")).IsEqualTo(3L);
            await Assert.That(await database.Scalar("SELECT count(*) FROM update_log")).IsEqualTo(2L);
            await Assert.That(log.Messages.Count(message => message.Contains("Direct outbox dispatch failed", StringComparison.Ordinal))).IsEqualTo(1);
        }
        finally { await Stop(worker); }
    }

    [Test]
    public async Task Completion_failure_keeps_delivered_rows_claimed_without_recording_send_failure()
    {
        await using var database = new Database();
        await database.Start();
        await using (var command = database.Source.CreateCommand($"""
            CREATE FUNCTION fail_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'completion unavailable'; END; $$;
            CREATE TRIGGER fail_completion BEFORE DELETE ON {Database.Table} FOR EACH STATEMENT EXECUTE FUNCTION fail_delete();
            """)) await command.ExecuteNonQueryAsync();
        var transport = new Transport();
        var log = new CaptureLogger();
        await using var provider = database.Services(transport, logger: log);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await database.Commit(provider, Record(1));
            await database.Commit(provider, Record(2));
            await Stop(worker);
            await Assert.That(transport.Received.Count).IsEqualTo(2);
            await Assert.That(await database.Scalar($"SELECT count(*) FROM {Database.Table} WHERE attempts = 0 AND last_error IS NULL AND claim_token IS NOT NULL")).IsEqualTo(2L);
            await Assert.That(log.Messages.Count(message => message.Contains("completion failed", StringComparison.Ordinal))).IsEqualTo(2);
        }
        finally { await Stop(worker); }
    }

    [Test]
    public async Task Expired_completion_logs_duplicate_delivery_risk_without_incrementing_attempts()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { Block = true };
        var log = new CaptureLogger();
        await using var provider = database.Services(transport, logger: log);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await database.Commit(provider, Record(1));
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            // Force expiry while the native send is blocked, without depending on elapsed time.
            await using (var command = database.Source.CreateCommand($"UPDATE {Database.Table} SET claim_expires_at = now() - interval '1 second'"))
                await command.ExecuteNonQueryAsync();
            transport.Release.TrySetResult();
            await Stop(worker);
            await Assert.That(transport.Received.Count).IsEqualTo(1);
            await Assert.That(await database.Scalar($"SELECT count(*) FROM {Database.Table} WHERE attempts = 0 AND last_error IS NULL")).IsEqualTo(1L);
            await Assert.That(log.Messages.Any(message => message.Contains("deleted 0 of 1 sent rows", StringComparison.Ordinal))).IsTrue();
        }
        finally { transport.Release.TrySetResult(); await Stop(worker); }
    }

    [Test]
    public async Task Shutdown_cancels_blocked_native_send_and_later_commits_remain_durable()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { Block = true };
        var log = new CaptureLogger();
        await using var provider = database.Services(transport, logger: log);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await database.Commit(provider, Record(1));
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await worker.StopAsync(cancellation.Token));
            await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));
            await database.Commit(provider, Record(2));
            await Assert.That(log.Messages.Any(message => message.Contains("queue is full or stopped", StringComparison.Ordinal))).IsTrue();
            await Assert.That(transport.Received.Count).IsEqualTo(0);
            await Assert.That(await database.Count()).IsEqualTo(2L);
        }
        finally { transport.Release.TrySetResult(); await Stop(worker); }
    }

    [Test]
    public async Task Competing_instances_claim_each_committed_row_once_under_normal_operation()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { Block = true };
        var options = new PostgreSqlOutboxOptions { Schema = Database.Schema, TablePrefix = "test_" };
        using var first = new PostgreSqlDirectDispatcher(database.Source, options, new(), [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        using var second = new PostgreSqlDirectDispatcher(database.Source, options, new(), [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        await using var provider = database.Services(transport, enableDispatch: false);
        var record = Record(1);
        await database.Commit(provider, record);
        var firstRows = new List<BufferedOutboxMessage> { new() };
        firstRows[0].Copy(record, 32);
        var secondRows = new List<BufferedOutboxMessage> { new() };
        secondRows[0].Copy(record, 32);
        await first.StartAsync(default);
        await second.StartAsync(default);
        try
        {
            first.Committed(firstRows, firstRows.Count);
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            // The first claimant holds a live claim while the other instance attempts the same row.
            second.Committed(secondRows, secondRows.Count);
            await Stop(second);
            await Assert.That(transport.Calls.Count).IsEqualTo(1);
            transport.Release.TrySetResult();
            await Stop(first);
            await Assert.That(transport.Received.Count).IsEqualTo(1);
            await Assert.That(await database.Count()).IsEqualTo(0L);
        }
        finally { transport.Release.TrySetResult(); await Stop(first); await Stop(second); }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Competing_claims_exclude_live_rows_without_misaligning_payloads(bool reverse)
    {
        await using var database = new Database();
        await database.Start();
        var blockedTransport = new Transport { Block = true };
        var competingTransport = new Transport();
        var options = new PostgreSqlOutboxOptions { Schema = Database.Schema, TablePrefix = "test_" };
        using var first = new PostgreSqlDirectDispatcher(database.Source, options, new(), [blockedTransport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        using var second = new PostgreSqlDirectDispatcher(database.Source, options, new(), [competingTransport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        await using var provider = database.Services(blockedTransport, enableDispatch: false);
        var held = Record(1);
        var available = Record(2);
        await database.Commit(provider, held);
        await database.Commit(provider, available);
        var firstRows = new List<BufferedOutboxMessage> { new() };
        firstRows[0].Copy(held, 32);
        var secondRows = new List<BufferedOutboxMessage> { new(), new() };
        secondRows[0].Copy(reverse ? available : held, 32);
        secondRows[1].Copy(reverse ? held : available, 32);
        await first.StartAsync(default);
        await second.StartAsync(default);
        try
        {
            first.Committed(firstRows, firstRows.Count);
            await blockedTransport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            second.Committed(secondRows, secondRows.Count);
            await Stop(second);
            await Assert.That(competingTransport.Received.Single().Body).IsEquivalentTo(new byte[] { 2 });
            await Assert.That(await database.Count()).IsEqualTo(1L);
            blockedTransport.Release.TrySetResult();
            await Stop(first);
            await Assert.That(blockedTransport.Received.Single().Body).IsEquivalentTo(new byte[] { 1 });
            await Assert.That(await database.Count()).IsEqualTo(0L);
        }
        finally { blockedTransport.Release.TrySetResult(); await Stop(first); await Stop(second); }
    }

    [Test]
    public async Task Full_queue_keeps_commit_successful_and_logs_pending_recovery_work()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { Block = true };
        var log = new CaptureLogger();
        await using var provider = database.Services(transport, new() { QueueCapacity = 1 }, logger: log);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await database.Commit(provider, Record(1));
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await database.Commit(provider, Record(2));
            await database.Commit(provider, Record(3));
            await Assert.That(log.Messages.Any(message => message.Contains("queue is full or stopped", StringComparison.Ordinal))).IsTrue();
            await Assert.That(await database.Count()).IsEqualTo(3L);
            transport.Release.TrySetResult();
            await Stop(worker);
            await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 1, 2 });
            await Assert.That(await database.Count()).IsEqualTo(1L);
        }
        finally { transport.Release.TrySetResult(); await Stop(worker); }
    }

    private static OutboxRecord Record(byte value, string destination = "destination", WireString? orderingKey = null,
        DateTimeOffset? deliverAt = null, KeyValuePair<string, WireString>[]? headers = null, byte[]? body = null,
        string transport = "events") =>
        new(Guid.CreateVersion7(), transport, "dispatch.probe;v=1",
            new(destination, body ?? [value], headers ?? [], orderingKey, deliverAt), DateTimeOffset.UtcNow);

    private static async Task Stop(PostgreSqlDirectDispatcher worker)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StopAsync(timeout.Token);
    }

    internal sealed class Database : IAsyncDisposable
    {
        internal const string Schema = "Dispatch\"Schema";
        internal const string Table = "\"Dispatch\"\"Schema\".\"test_outbox\"";
        private readonly PostgreSqlFixture _fixture = new();
        internal NpgsqlDataSource Source { get; private set; } = null!;

        internal async Task Start()
        {
            await _fixture.StartAsync();
            Source = NpgsqlDataSource.Create(_fixture.ConnectionString);
            await using var command = Source.CreateCommand(PostgreSqlOutboxSchema.CreateScript(new() { Schema = Schema, TablePrefix = "test_" }) + $"""
                CREATE TABLE delete_log (id integer);
                CREATE FUNCTION log_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO delete_log VALUES (1); RETURN NULL; END; $$;
                CREATE TRIGGER delete_count AFTER DELETE ON {Table} FOR EACH STATEMENT EXECUTE FUNCTION log_delete();
                CREATE TABLE update_log (id integer);
                CREATE FUNCTION log_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO update_log VALUES (1); RETURN NULL; END; $$;
                CREATE TRIGGER update_count AFTER UPDATE ON {Table} FOR EACH STATEMENT EXECUTE FUNCTION log_update();
                """);
            await command.ExecuteNonQueryAsync();
        }

        internal ServiceProvider Services(Transport transport, PostgreSqlDirectDispatchOptions? options = null,
            bool enableDispatch = true, CaptureLogger? logger = null)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<ILogger<PostgreSqlDirectDispatcher>>(logger ?? new CaptureLogger());
            services.AddSingleton<ITransport>(transport);
            services.AddSingleton<IMessageSerializer, Serializer>();
            services.AddRelayerEntityFrameworkCoreOutbox();
            services.AddRelayerPostgreSqlMaintenance("schedules", Source, new PostgreSqlMaintenanceOptions { Schema = Schema, TablePrefix = "test_" });
            if (enableDispatch) services.AddRelayerPostgreSqlDirectDispatch("store", Source,
                new() { Schema = Schema, TablePrefix = "test_" }, options);
            services.AddDbContext<TestContext>((provider, builder) => builder.UseNpgsql(_fixture.ConnectionString)
                .AddRelayerOutbox(provider, new EntityFrameworkCoreOutboxOptions
                {
                    Schema = Schema,
                    TablePrefix = "test_",
                    DispatchStoreName = enableDispatch ? "store" : null
                }));
            services.AddRelayer(builder =>
            {
                builder.Infrastructure().MessageTypes.Add<Probe>("dispatch.probe");
                builder.AddTransport<Empty, Empty>("events", routes => routes.Send<Probe>("destination"));
            });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }

        internal async Task Commit(ServiceProvider provider, OutboxRecord record)
        {
            await using var scope = provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<TestContext>();
            using var selection = db.EnlistRelayerOutbox();
            await scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().EnlistAsync(record, default);
            await db.SaveChangesAsync();
        }

        internal Task<long> Count() => Scalar($"SELECT count(*) FROM {Table}");
        internal async Task<long> Scalar(string sql)
        {
            await using var command = Source.CreateCommand(sql);
            return Convert.ToInt64(await command.ExecuteScalarAsync());
        }

        public async ValueTask DisposeAsync()
        {
            if (Source is not null) await Source.DisposeAsync();
            await _fixture.DisposeAsync();
        }
    }

    public sealed class TestContext(DbContextOptions<TestContext> options) : DbContext(options);
    public sealed class Empty;
    [Message("dispatch.probe")]
    public sealed record Probe(byte Value);

    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        { destination.GetSpan(1)[0] = ((Probe)(object)message!).Value; destination.Advance(1); }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }

    internal sealed class CaptureLogger : ILogger<PostgreSqlDirectDispatcher>
    {
        internal ConcurrentQueue<string> Messages { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Messages.Enqueue(formatter(state, exception));
        }
    }
}
