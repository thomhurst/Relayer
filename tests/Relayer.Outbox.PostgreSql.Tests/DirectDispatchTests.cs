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
    [Arguments(false)]
    [Arguments(true)]
    public async Task Explicit_transactions_dispatch_only_after_commit_and_rollback_never_sends(bool synchronous)
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
            await using (var transaction = synchronous ? db.Database.BeginTransaction() : await db.Database.BeginTransactionAsync())
            {
                await sender.SendAsync(new Probe(1));
                if (synchronous) db.SaveChanges(); else await db.SaveChangesAsync();
                if (synchronous) transaction.Rollback(); else await transaction.RollbackAsync();
            }
            await using (var transaction = synchronous ? db.Database.BeginTransaction() : await db.Database.BeginTransactionAsync())
            {
                await sender.SendAsync(new Probe(2));
                if (synchronous) db.SaveChanges(); else await db.SaveChangesAsync();
                await Assert.That(transport.Entered.Task.IsCompleted).IsFalse();
                await Assert.That(await database.Count()).IsEqualTo(0L);
                if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
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
        }
        finally { await Stop(worker); }
    }

    [Test]
    public async Task Ordered_and_future_rows_remain_durable_while_due_unordered_rows_dispatch()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport();
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
                await outbox.EnlistAsync(Record(1, orderingKey: new WireString("")), default);
                await outbox.EnlistAsync(Record(2, deliverAt: DateTimeOffset.UtcNow.AddDays(1)), default);
                await outbox.EnlistAsync(Record(3, deliverAt: DateTimeOffset.UtcNow.AddDays(-1)), default);
                await db.SaveChangesAsync();
            }
            await Stop(worker);
            await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 3 });
            await Assert.That(await database.Count()).IsEqualTo(2L);
            await Assert.That(await database.Scalar($"SELECT count(*) FROM {Database.Table} WHERE claim_token IS NOT NULL")).IsEqualTo(0L);
        }
        finally { await Stop(worker); }
    }

    [Test]
    public async Task Native_failure_is_recorded_once_and_does_not_prevent_next_commit_dispatch()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { FailFirst = 1 };
        await using var provider = database.Services(transport);
        var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
        await worker.StartAsync(default);
        try
        {
            await database.Commit(provider, Record(1));
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await database.Commit(provider, Record(2));
            await Stop(worker);
            await Assert.That(transport.Calls.Count).IsEqualTo(2);
            await Assert.That(transport.Received.Select(sent => sent.Body[0])).IsEquivalentTo(new byte[] { 2 });
            await Assert.That(await database.Count()).IsEqualTo(1L);
            await Assert.That(await database.Scalar($"SELECT attempts FROM {Database.Table}")).IsEqualTo(1L);
            await using var command = database.Source.CreateCommand($"SELECT last_error FROM {Database.Table}");
            await Assert.That((string)(await command.ExecuteScalarAsync())!).IsEqualTo("native send failed");
            await Assert.That(await database.Scalar($"SELECT count(*) FROM {Database.Table} WHERE claim_token IS NOT NULL")).IsEqualTo(0L);
        }
        finally { await Stop(worker); }
    }

    [Test]
    public async Task Partial_native_batch_failure_preserves_every_row_for_at_least_once_recovery()
    {
        await using var database = new Database();
        await database.Start();
        var transport = new Transport { PartialFirst = 1 };
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
                await outbox.EnlistAsync(Record(1), default);
                await outbox.EnlistAsync(Record(2), default);
                await db.SaveChangesAsync();
            }
            await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            await database.Commit(provider, Record(3));
            await Stop(worker);
            await Assert.That(transport.Calls.Select(call => call.Count)).IsEquivalentTo(new[] { 2, 1 });
            await Assert.That(transport.Received.Count).IsEqualTo(2);
            await Assert.That(transport.Received.Count(sent => sent.Body[0] == 3)).IsEqualTo(1);
            await Assert.That(transport.Received.Count(sent => sent.Body[0] is 1 or 2)).IsEqualTo(1);
            await Assert.That(await database.Count()).IsEqualTo(2L);
            await Assert.That(await database.Scalar($"SELECT count(*) FROM {Database.Table} WHERE attempts = 1 AND claim_token IS NULL")).IsEqualTo(2L);
        }
        finally { await Stop(worker); }
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
        DateTimeOffset? deliverAt = null, KeyValuePair<string, WireString>[]? headers = null, byte[]? body = null) =>
        new(Guid.CreateVersion7(), "events", "dispatch.probe;v=1",
            new(destination, body ?? [value], headers ?? [], orderingKey, deliverAt), DateTimeOffset.UtcNow);

    private static async Task Stop(PostgreSqlDirectDispatcher worker)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await worker.StopAsync(timeout.Token);
    }

    private sealed class Database : IAsyncDisposable
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
                builder.MessageTypes.Add<Probe>("dispatch.probe");
                builder.AddTransport<Empty, Empty, Empty>("events", routes => routes.Send<Probe>("destination"));
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

    private sealed record Sent(byte[] Body, KeyValuePair<string, byte[]>[] Headers);
    private sealed record NativeCall(string Destination, int Count);

    private sealed class Transport : ITransport
    {
        internal bool Block { get; init; }
        internal int FailFirst;
        internal int PartialFirst;
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal ConcurrentQueue<Sent> Received { get; } = new();
        internal ConcurrentQueue<NativeCall> Calls { get; } = new();
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true, MaxBatchSize = endpoint == "single" ? 1 : 2 };

        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Calls.Enqueue(new(message.Destination, 1));
            Entered.TrySetResult();
            if (Block) await Release.Task.WaitAsync(cancellationToken);
            if (Interlocked.Exchange(ref FailFirst, 0) != 0) throw new InvalidOperationException("native send failed");
            Record(message);
        }

        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            Calls.Enqueue(new(messages.Span[0].Destination, messages.Length));
            Entered.TrySetResult();
            if (Block) await Release.Task.WaitAsync(cancellationToken);
            if (Interlocked.Exchange(ref PartialFirst, 0) != 0)
            {
                Record(messages.Span[0]);
                throw new InvalidOperationException("native send failed");
            }
            if (Interlocked.Exchange(ref FailFirst, 0) != 0) throw new InvalidOperationException("native send failed");
            for (var i = 0; i < messages.Length; i++) Record(messages.Span[i]);
        }

        private void Record(OutgoingMessage message)
        {
            // Like Kafka, this endpoint has no native scheduling. The outbox already checked the due time.
            if (message.DeliverAt is not null) throw new NotSupportedException("Native scheduling is unavailable.");
            var headers = new KeyValuePair<string, byte[]>[message.Headers.Length];
            for (var i = 0; i < headers.Length; i++)
            {
                var header = message.Headers.Span[i];
                headers[i] = new(header.Key, header.Value.Text is { } text ? System.Text.Encoding.UTF8.GetBytes(text) : header.Value.Utf8.ToArray());
            }
            Received.Enqueue(new(message.Body.ToArray(), headers));
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }

    private sealed class CaptureLogger : ILogger<PostgreSqlDirectDispatcher>
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
