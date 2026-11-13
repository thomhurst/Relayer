using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class SerializableIsolationTests
{
    private const int Writers = 16;
    private const int Batches = 32;

    [Test]
    [Arguments("ef-outbox")]
    [Arguments("ado-outbox")]
    [Arguments("ef-inbox-outbox")]
    public async Task Unrelated_serializable_writes_add_no_conflicts(string path)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        { Pooling = true, MaxPoolSize = Writers + 8 }.ConnectionString);
        await CreateTables(source);

        // Both arms run identical business work, snapshot/commit barriers and maintenance.
        // The transaction-start counter rejects replay, so retries cannot hide conflicts.
        var control = await Run(source, path, relayer: false);
        var treatment = await Run(source, path, relayer: true);
        Console.WriteLine($"Serializable {path}: {Writers * Batches} attempts/arm; control {control}; Relayer {treatment}");
        await Assert.That(control.SerializationFailures).IsEqualTo(0);
        await Assert.That(control.Deadlocks).IsEqualTo(0);
        await Assert.That(treatment.SerializationFailures).IsEqualTo(control.SerializationFailures);
        await Assert.That(treatment.Deadlocks).IsEqualTo(control.Deadlocks);
        await Assert.That(treatment.Committed).IsEqualTo(Writers * Batches);
    }

    [Test]
    public async Task Counter_detects_real_serialization_failures_without_replaying()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        { Pooling = true, MaxPoolSize = Writers + 8 }.ConnectionString);
        await CreateTables(source);
        // Read the next writer's row before updating our own: a real Serializable
        // dependency cycle proves that the counter does not mask aborted attempts.
        var result = await Run(source, "ef-outbox", relayer: false, conflictProbe: true);
        await Assert.That(result.SerializationFailures).IsGreaterThan(0);
        await Assert.That(result.Deadlocks).IsEqualTo(0);
        await Assert.That(result.Committed + result.SerializationFailures).IsEqualTo(Writers * Batches);
    }

    private static async Task CreateTables(NpgsqlDataSource source)
    {
        await using var create = source.CreateCommand(PostgreSqlOutboxSchema.CreateScript() + """
            CREATE TABLE business ("Id" integer PRIMARY KEY, "Value" integer NOT NULL) WITH (fillfactor = 50);
            INSERT INTO business SELECT id, 0 FROM generate_series(0, 16383) id;
            ANALYZE business;
            """);
        await create.ExecuteNonQueryAsync();
    }

    private static async Task<Counts> Run(NpgsqlDataSource source, string path, bool relayer, bool conflictProbe = false)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var cancellation = timeout.Token;
        var state = new Workload { Relayer = relayer, ConflictProbe = conflictProbe };
        await using var provider = Services(source, state);
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
            .EntityFrameworkCore<BusinessContext>("serializable", inbox: relayer,
                isolationLevel: IsolationLevel.Serializable).Build();
        var transport = new Transport();
        using var direct = new PostgreSqlDirectDispatcher(source, new(), new(), [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        using var maintenance = new PostgreSqlMaintenanceWorker(source,
            new() { RecoveryDelay = TimeSpan.Zero, InboxRetention = TimeSpan.FromDays(1) },
            [transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        var failures = new ConcurrentQueue<string>();
        var committed = 0;
        var cleaned = 0;
        try
        {
            for (var batch = 0; batch < Batches; batch++)
            {
                state.Batch = new Batch();
                // Seed genuine recovery and retention work, including in the control arm.
                var seed = Record();
                await Seed(source, seed, cancellation);
                var attempts = Enumerable.Range(0, Writers).Select(Attempt).ToArray();
                try
                {
                    await state.Batch.Prepared.Task.WaitAsync(cancellation);
                    // These are the real claim/send/delete and retention implementations, run
                    // while every successful writer still holds its Serializable transaction.
                    await Task.WhenAll(direct.DispatchRecoveredAsync(new[] { seed }, cancellation,
                        propagateDatabaseFailures: true).AsTask(), Maintain());
                }
                finally
                {
                    state.Batch.Commit.TrySetResult();
                    await Task.WhenAll(attempts);
                }
            }
            // A sweep uses a fixed high-water mark and deliberately wraps on a later
            // pass. Drain the final committed batch through the worker's full scan.
            await maintenance.DispatchCommittedAsync(cancellation);
            await using var pending = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
            await Assert.That((long)(await pending.ExecuteScalarAsync(cancellation))!).IsEqualTo(0L);
            await using var durable = source.CreateCommand("SELECT sum(\"Value\")::bigint FROM business");
            await Assert.That((long)(await durable.ExecuteScalarAsync(cancellation))!).IsEqualTo(committed);
            await using var inbox = source.CreateCommand("SELECT count(*) FROM relayer_inbox WHERE consumer = 'serializable'");
            await Assert.That((long)(await inbox.ExecuteScalarAsync(cancellation))!).IsEqualTo(relayer && path == "ef-inbox-outbox" ? committed : 0);
            await Assert.That(cleaned).IsEqualTo(Batches);
            await Assert.That(transport.Sends).IsEqualTo(Batches + (relayer ? committed : 0));
            await using var reset = source.CreateCommand("UPDATE business SET \"Value\" = 0; TRUNCATE relayer_inbox");
            await reset.ExecuteNonQueryAsync(cancellation);
            return new(committed, failures.Count(x => x == PostgresErrorCodes.SerializationFailure),
                failures.Count(x => x == PostgresErrorCodes.DeadlockDetected));
        }
        finally { await maintenance.ReleaseAsync(CancellationToken.None); }

        async Task Maintain()
        {
            await maintenance.SweepAsync(cancellation);
            cleaned += await maintenance.CleanupAsync(cancellation);
        }

        async Task Attempt(int writer)
        {
            try
            {
                if (path == "ef-inbox-outbox")
                {
                    using var context = pipeline.RentContext(new ConsumeMessage
                    {
                        MessageType = new("serializable.probe", 1), MessageId = Guid.CreateVersion7().ToString("D"),
                        Body = new ReadOnlySequence<byte>(new[] { (byte)writer })
                    }, cancellation);
                    await pipeline.ProcessAsync(context);
                }
                else if (path == "ef-outbox")
                {
                    await using var scope = provider.CreateAsyncScope();
                    var db = scope.ServiceProvider.GetRequiredService<BusinessContext>();
                    await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.Serializable, cancellation);
                    await Write(db, scope.ServiceProvider, writer, state, cancellation);
                    await transaction.CommitAsync(cancellation);
                }
                else
                {
                    await using var connection = await source.OpenConnectionAsync(cancellation);
                    await using var outbox = new PostgreSqlOutbox(new Snapshot());
                    await using var transaction = relayer
                        ? await outbox.BeginTransactionAsync(connection, IsolationLevel.Serializable, cancellation)
                        : await connection.BeginTransactionAsync(IsolationLevel.Serializable, cancellation);
                    await state.Batch.Snapshot(transaction.Connection!, transaction, cancellation);
                    await using var update = transaction.Connection!.CreateCommand();
                    update.Transaction = transaction;
                    update.CommandText = "UPDATE business SET \"Value\" = \"Value\" + 1 WHERE \"Id\" = @id";
                    var parameter = update.CreateParameter();
                    parameter.ParameterName = "id";
                    parameter.Value = writer * 1024;
                    update.Parameters.Add(parameter);
                    await Assert.That(await update.ExecuteNonQueryAsync(cancellation)).IsEqualTo(1);
                    if (relayer) await outbox.EnlistAsync(Record(), cancellation);
                    await state.Batch.Prepare(writer, cancellation);
                    await transaction.CommitAsync(cancellation);
                }
                Interlocked.Increment(ref committed);
            }
            catch (Exception error) when (Conflict(error) is { } sqlState)
            {
                failures.Enqueue(sqlState);
            }
            finally { state.Batch.ReportPrepared(writer); }
        }
    }

    private static string? Conflict(Exception error)
    {
        for (Exception? current = error; current is not null; current = current.InnerException)
            if (current is PostgresException { SqlState: PostgresErrorCodes.SerializationFailure or PostgresErrorCodes.DeadlockDetected } postgres)
                return postgres.SqlState;
        return null;
    }

    private static ServiceProvider Services(NpgsqlDataSource source, Workload state)
    {
        var services = new ServiceCollection();
        services.AddSingleton(state);
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddRelayerEntityFrameworkCoreOutbox();
        services.AddDbContext<BusinessContext>((sp, options) =>
        {
            options.UseNpgsql(source).AddInterceptors(new SerializableTransactions(state));
            if (state.Relayer) options.AddRelayerOutbox(sp);
        });
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Probe>("serializable.probe");
            new ConsumerBuilder<object>(builder.Services, builder.Handlers, "manual").Handler<Handler>();

        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static async Task Write(BusinessContext db, IServiceProvider services, int writer, Workload state, CancellationToken cancellation)
    {
        if (state.ConflictProbe)
            _ = await db.Set<Business>().FindAsync(new object[] { (writer + 1) % Writers * 1024 }, cancellation);
        var row = await db.Set<Business>().FindAsync(new object[] { writer * 1024 }, cancellation);
        row!.Value++;
        if (state.Relayer) await services.GetRequiredService<IOutboxEnlistment>().EnlistAsync(Record(), cancellation);
        await db.SaveChangesAsync(cancellation);
        await state.Batch.Prepare(writer, cancellation);
    }

    private static OutboxRecord Record() => new(Guid.CreateVersion7(), "events", "serializable.probe;v=1",
        new("destination", new byte[] { 1 }), DateTimeOffset.UtcNow);

    private static async Task Seed(NpgsqlDataSource source, OutboxRecord record, CancellationToken cancellation)
    {
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, []);
        await using var command = source.CreateCommand("""
            INSERT INTO relayer_outbox (id, transport, destination, message_type, headers, body, created_at)
            VALUES (@id, 'events', 'destination', 'serializable.probe;v=1', @headers, @body, clock_timestamp() - interval '1 day');
            INSERT INTO relayer_inbox VALUES ('retention', @id, clock_timestamp() - interval '2 days');
            """);
        command.Parameters.AddWithValue("id", record.Id);
        command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
        command.Parameters.AddWithValue("body", record.Message.Body.ToArray());
        await command.ExecuteNonQueryAsync(cancellation);
    }

    // Observe native transactions without replacing the transaction or changing isolation.
    private sealed class SerializableTransactions(Workload state) : DbTransactionInterceptor
    {
        public override async ValueTask<DbTransaction> TransactionStartedAsync(DbConnection connection, TransactionEndEventData eventData,
            DbTransaction result, CancellationToken cancellationToken = default)
        {
            await state.Batch.Snapshot(connection, result, cancellationToken);
            return result;
        }
    }

    internal sealed class Batch
    {
        private int _snapshots;
        private int _prepared;
        private readonly int[] _reported = new int[Writers];
        private readonly TaskCompletionSource _start = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Prepared { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Commit { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal async Task Snapshot(DbConnection connection, DbTransaction transaction, CancellationToken cancellation)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SET LOCAL enable_seqscan = off; SELECT current_setting('transaction_isolation'), pg_current_snapshot()::text";
            await using (var reader = await command.ExecuteReaderAsync(cancellation))
            {
                await reader.ReadAsync(cancellation);
                await Assert.That(reader.GetString(0)).IsEqualTo("serializable");
            }
            var started = Interlocked.Increment(ref _snapshots);
            if (started > Writers)
                throw new InvalidOperationException("A transaction was replayed. Conflict retries must not hide rejected Serializable attempts.");
            if (started == Writers) _start.TrySetResult();
            await _start.Task.WaitAsync(cancellation);
        }

        internal void ReportPrepared(int writer)
        {
            if (Interlocked.Exchange(ref _reported[writer], 1) == 0 && Interlocked.Increment(ref _prepared) == Writers)
                Prepared.TrySetResult();
        }

        internal async Task Prepare(int writer, CancellationToken cancellation)
        {
            ReportPrepared(writer);
            await Commit.Task.WaitAsync(cancellation);
        }
    }

    public sealed class Workload
    {
        public bool Relayer { get; init; }
        public bool ConflictProbe { get; init; }
        internal Batch Batch { get; set; } = new();
    }

    private sealed record Counts(int Committed, int SerializationFailures, int Deadlocks);
    private sealed class Snapshot : IOptionsSnapshot<PostgreSqlOutboxOptions>
    {
        public PostgreSqlOutboxOptions Value { get; } = new();
        public PostgreSqlOutboxOptions Get(string? name) => Value;
    }

    public sealed class BusinessContext(DbContextOptions<BusinessContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<Business>().ToTable("business").HasKey(x => x.Id);
    }
    public sealed class Business { public int Id { get; set; } public int Value { get; set; } }
    [Message("serializable.probe")]
    public sealed record Probe(int Writer);
    public sealed class Handler(BusinessContext db, Workload state) : IHandler<Probe>
    {
        public async ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
            => await Write(db, context.Services, message.Writer, state, cancellationToken);
    }
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)new Probe(source.FirstSpan[0]);
    }
    private sealed class Transport : ITransport
    {
        public int Sends;
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 1 };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Interlocked.Increment(ref Sends); return default; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
