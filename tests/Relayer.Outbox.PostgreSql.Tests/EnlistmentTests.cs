using System.Buffers;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class EnlistmentTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Implicit_saves_and_explicit_commits_are_atomic_and_batched(bool synchronous)
    {
        await using var database = new Database();
        await database.Start();
        await using var schedulerSource = NpgsqlDataSource.Create(database.ConnectionString);
        await using var provider = database.Services(schedulerSource: schedulerSource);
        var scheduled = DateTimeOffset.UtcNow.AddDays(1);
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            // No transaction: native sending still works.
            await sender.SendAsync(new Probe(9));
            await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(1);
            using var enlistment = db.EnlistRelayerOutbox();
            db.Add(new Order { Id = 1 });
            await sender.SendAsync(new Probe(1), o => { o.OrderingKey = "café"; o.DeliverAt = scheduled; o.Headers["custom"] = "é"; });
            await sender.SendViaOutboxAsync(new Probe(2));
            await Assert.That(await database.Count()).IsEqualTo(0L);
            if (synchronous) db.SaveChanges(); else await db.SaveChangesAsync();
            await Assert.That(await database.Count()).IsEqualTo(2L);
            await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(1L);
            await Assert.That(await database.Scalar("SELECT count(*) FROM batch_log")).IsEqualTo(1L);

            // An outbox-only save must also create and commit a transaction.
            await sender.SendAsync(new Probe(3));
            if (synchronous) db.SaveChanges(); else await db.SaveChangesAsync();
        }
        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await using var transaction = synchronous ? db.Database.BeginTransaction() : await db.Database.BeginTransactionAsync();
            await sender.SendAsync(new Probe(4));
            db.Add(new Order { Id = 2 });
            if (synchronous) db.SaveChanges(); else await db.SaveChangesAsync();
            await sender.SendAsync(new Probe(5));
            if (synchronous) db.SaveChanges(); else await db.SaveChangesAsync();
            await sender.SendAsync(new Probe(6)); // No final SaveChanges required for these rows.
            await Assert.That(await database.Count()).IsEqualTo(3L);
            if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
            await Assert.That(scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().IsActive).IsFalse();
        }
        await Assert.That(await database.Count()).IsEqualTo(6L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM batch_log")).IsEqualTo(3L);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(1);
        await database.VerifyEnvelope(scheduled);
    }

    [Test]
    public async Task Rollback_disposal_cancellation_and_failed_saves_never_leak_messages()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            await sender.SendAsync(new Probe(1));
            db.Add(new Order { Id = 1 });
            await db.SaveChangesAsync();
            await transaction.RollbackAsync();
        }
        await using (await db.Database.BeginTransactionAsync()) await sender.SendAsync(new Probe(2));
        await Assert.That(scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().IsActive).IsFalse();
        using (db.EnlistRelayerOutbox())
        {
            await sender.SendAsync(new Probe(3));
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            await Assert.That(async () => await db.SaveChangesAsync(cancellation.Token)).Throws<OperationCanceledException>();
        }
        db.ChangeTracker.Clear();
        using (db.EnlistRelayerOutbox())
        {
            db.Add(new Order { Id = -1 }); // Database CHECK violation.
            await sender.SendAsync(new Probe(4));
            await Assert.That(async () => await db.SaveChangesAsync()).Throws<DbUpdateException>();
        }
        db.ChangeTracker.Clear();
        // A failed explicit unit cannot commit its earlier buffered sends after a savepoint rollback.
        await using (var transaction = await db.Database.BeginTransactionAsync())
        {
            await sender.SendAsync(new Probe(5));
            db.Add(new Order { Id = -2 });
            await Assert.That(async () => await db.SaveChangesAsync()).Throws<DbUpdateException>();
            await Assert.That(async () => await transaction.CommitAsync()).Throws<InvalidOperationException>();
            await transaction.RollbackAsync();
        }
        db.ChangeTracker.Clear();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(0L);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
        // Discarded sends never reappear in a later successful unit.
        using (db.EnlistRelayerOutbox())
        {
            await sender.SendAsync(new Probe(6));
            await db.SaveChangesAsync();
        }
        await Assert.That(await database.Count()).IsEqualTo(1L);
    }

    [Test]
    public async Task Multiple_contexts_select_their_owner_and_shared_transactions_use_one_insert()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var orders = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        var audits = scope.ServiceProvider.GetRequiredService<AuditContext>();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        using (orders.EnlistRelayerOutbox())
        {
            // Nested selection of the same context must not discard the outer unit's sends.
            using (orders.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(1));
            // Saving another context must not steal the buffered row.
            await audits.SaveChangesAsync();
            await Assert.That(await database.Count()).IsEqualTo(0L);
            await orders.SaveChangesAsync();
        }
        await using (var first = await orders.Database.BeginTransactionAsync())
        await using (var second = await audits.Database.BeginTransactionAsync())
        {
            await Assert.That(async () => await sender.SendAsync(new Probe(2))).Throws<InvalidOperationException>();
            using (orders.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(3));
            using (audits.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(4));
            orders.Add(new Order { Id = 10 });
            audits.Add(new Order { Id = 20 });
            await orders.SaveChangesAsync();
            await audits.SaveChangesAsync();
            await first.CommitAsync();
            await second.RollbackAsync();
        }
        await Assert.That(await database.Count()).IsEqualTo(2L);

        // EF's UseTransaction joins the same native transaction across contexts.
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<AuditContext>();
        options.UseNpgsql(connection).AddRelayerOutbox(scope.ServiceProvider, Database.Options);
        await using var joined = new AuditContext(options.Options);
        orders.Database.SetDbConnection(connection);
        await using var shared = await orders.Database.BeginTransactionAsync();
        await joined.Database.UseTransactionAsync(shared.GetDbTransaction());
        using (orders.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(5));
        using (joined.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(6));
        using (orders.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(7));
        orders.Add(new Order { Id = 30 });
        joined.Add(new Order { Id = 40 });
        await orders.SaveChangesAsync();
        await joined.SaveChangesAsync();
        await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(1L);
        await shared.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(5L);
        await Assert.That(await database.Scalar($"SELECT (string_agg(get_byte(body, 0)::text, ',' ORDER BY seq) = '1,3,5,6,7')::int::bigint FROM {Database.Table}"))
            .IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM batch_log")).IsEqualTo(3L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(3L);
    }

    [Test]
    public async Task Insert_failure_rolls_back_business_data_and_borrowed_storage_is_copied()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        byte[] body = [1, 2, 3];
        byte[] value = "héllo"u8.ToArray();
        byte[] key = "key"u8.ToArray();
        KeyValuePair<string, WireString>[] headers = [new("custom", new WireString(value))];
        var message = new OutgoingMessage("target", body, headers, new WireString(key));
        var record = new OutboxRecord(Guid.CreateVersion7(), "events", "wire.type;v=2", message, DateTimeOffset.UtcNow, TenantId: "tenant-a");
        using (db.EnlistRelayerOutbox())
        {
            await outbox.EnlistAsync(record, default);
            body[0] = 9; value[0] = 9; key[0] = 9; headers[0] = default;
            await db.SaveChangesAsync();
        }
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT body, headers, ordering_key, tenant_id, lane FROM {Database.Table}", connection);
        int lane;
        await using (var reader = await command.ExecuteReaderAsync())
        {
            await reader.ReadAsync();
            await Assert.That(reader.GetFieldValue<byte[]>(0).SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
            await Assert.That(DecodeHeaders(reader.GetFieldValue<byte[]>(1))[0]).IsEqualTo(new KeyValuePair<string, string>("custom", "héllo"));
            await Assert.That(reader.GetString(2)).IsEqualTo("key");
            await Assert.That(reader.GetString(3)).IsEqualTo("tenant-a");
            lane = reader.GetInt32(4);
        }
        await using var dataSource = NpgsqlDataSource.Create(database.ConnectionString);
        var store = new PostgreSqlOutboxStore(dataSource, new() { Schema = Database.Options.Schema, TablePrefix = Database.Options.TablePrefix });
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(lane, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(record.Id);
        await Assert.That(rows[0].MessageType).IsEqualTo(record.MessageType);
        await Assert.That(rows[0].TenantId).IsEqualTo("tenant-a");
        await Assert.That(rows[0].Message.Headers.Span[0].Value.ToString()).IsEqualTo("héllo");
        await Assert.That(await store.GetPendingCountAsync(DateTimeOffset.UtcNow, default)).IsEqualTo(1L);
        // Duplicate original identity makes the single INSERT fail. Business data must roll back as well.
        record = record with { Message = new OutgoingMessage("target", default) };
        using (db.EnlistRelayerOutbox())
        {
            await outbox.EnlistAsync(record, default);
            db.Add(new Order { Id = 1 });
            await Assert.That(async () => await db.SaveChangesAsync()).Throws<PostgresException>();
        }
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(0L);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    public async Task Large_transaction_uses_one_insert_with_a_fixed_parameter_count()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await using var transaction = await db.Database.BeginTransactionAsync();
        var record = new OutboxRecord(default, "events", "wire.type;v=1", new("target", new byte[256]), DateTimeOffset.UtcNow);
        // A per-row parameter strategy would exceed PostgreSQL's 65,535 parameter limit.
        for (var i = 0; i < 7000; i++) await outbox.EnlistAsync(record with { Id = Guid.CreateVersion7() }, default);
        await transaction.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(7000L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM batch_log")).IsEqualTo(1L);
    }

    [Test]
    public async Task Invalid_configuration_and_nonpersistable_messages_fail_early()
    {
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        await Assert.That(() => new DbContextOptionsBuilder().AddRelayerOutbox(scope.ServiceProvider,
            new() { Lanes = 0 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new DbContextOptionsBuilder().AddRelayerOutbox(scope.ServiceProvider,
            new() { Schema = "" })).Throws<ArgumentException>();
        using var context = new DbContext(new DbContextOptionsBuilder().Options);
        await Assert.That(() => context.EnlistRelayerOutbox()).Throws<InvalidOperationException>();

        var options = new DbContextOptionsBuilder<OrdersContext>();
        options.UseNpgsql("Host=unused").AddRelayerOutbox(scope.ServiceProvider);
        using var configured = new OrdersContext(options.Options);
        await Assert.That(() => options.AddRelayerOutbox(scope.ServiceProvider)).Throws<InvalidOperationException>();
        using var enlistment = configured.EnlistRelayerOutbox();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        var record = new OutboxRecord(default, "events", "wire.type;v=1", new("target", default), DateTimeOffset.UtcNow);
        await Assert.That(async () => await outbox.EnlistAsync(record, default)).Throws<InvalidOperationException>();
        var native = new SendOptions();
        native.ConfigureNative<object>(static _ => { });
        await Assert.That(async () => await outbox.EnlistAsync(record with { Message = new("target", default, options: native) }, default)).Throws<InvalidOperationException>();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await outbox.EnlistAsync(record, cancellation.Token)).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Immutable_options_bind_validate_and_reload_for_new_scopes()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        { ["Schema"] = "messages", ["TablePrefix"] = "orders_", ["Lanes"] = "7" }).Build();
        var services = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox(configuration);
        await using var provider = services.BuildServiceProvider();
        using var first = provider.CreateScope();
        var options = first.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value;
        await Assert.That(options.Schema).IsEqualTo("messages");
        await Assert.That(options.TablePrefix).IsEqualTo("orders_");
        await Assert.That(options.Lanes).IsEqualTo(7);
        configuration["Lanes"] = "11";
        configuration.Reload();
        using var second = provider.CreateScope();
        await Assert.That(second.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value.Lanes).IsEqualTo(11);
        await Assert.That(first.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value.Lanes).IsEqualTo(7);
        configuration["Lanes"] = "bad";
        using var invalid = provider.CreateScope();
        await Assert.That(() => invalid.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value)
            .Throws<OptionsValidationException>();
        configuration["Lanes"] = "0";
        using var zero = provider.CreateScope();
        await Assert.That(() => zero.ServiceProvider.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value)
            .Throws<OptionsValidationException>();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Disposing_shared_participants_preserves_rows_until_owner_commits(bool synchronous, bool disposeContext)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        await using var connection = new NpgsqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        var orders = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        orders.Database.SetDbConnection(connection);
        var options = new DbContextOptionsBuilder<AuditContext>();
        options.UseNpgsql(connection).AddRelayerOutbox(scope.ServiceProvider, Database.Options);
        await using var joined = new AuditContext(options.Options);
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await using var transaction = await orders.Database.BeginTransactionAsync();
        var wrapper = await joined.Database.UseTransactionAsync(transaction.GetDbTransaction());
        using (joined.EnlistRelayerOutbox())
        {
            await sender.SendAsync(new Probe(1));
            joined.Add(new Order { Id = 1 });
            await joined.SaveChangesAsync();
        }
        if (disposeContext)
        {
            if (synchronous) joined.Dispose(); else await joined.DisposeAsync();
        }
        else
        {
            if (synchronous) wrapper!.Dispose(); else await wrapper!.DisposeAsync();
            await Assert.That(() => joined.EnlistRelayerOutbox()).Throws<InvalidOperationException>();
        }
        // Automatic routing must ignore the detached participant without discarding its durable work.
        await sender.SendAsync(new Probe(2));
        if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(2L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM batch_log")).IsEqualTo(1L);
        await Assert.That(scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().IsActive).IsFalse();
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    public async Task Disposed_shared_units_do_not_reappear_when_the_native_transaction_is_reused()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        await db.Database.OpenConnectionAsync();
        var options = new DbContextOptionsBuilder<AuditContext>();
        options.UseNpgsql(db.Database.GetDbConnection()).AddRelayerOutbox(scope.ServiceProvider, Database.Options);
        await using var joined = new AuditContext(options.Options);
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await using (var abandoned = await db.Database.BeginTransactionAsync())
        {
            await joined.Database.UseTransactionAsync(abandoned.GetDbTransaction());
            using (joined.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(1));
        }
        // No send or IsActive probe between disposal and the next BeginTransaction.
        await using (var next = await db.Database.BeginTransactionAsync())
        {
            using (db.EnlistRelayerOutbox()) await sender.SendAsync(new Probe(2));
            await next.CommitAsync();
        }
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar($"SELECT sum(get_byte(body, 0)) FROM {Database.Table}")).IsEqualTo(2L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Retrying_strategies_require_explicit_transactions_and_retry_complete_units(bool synchronous)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(retries: true);
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<OrdersContext>();
        using (db.EnlistRelayerOutbox())
        {
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(1));
            db.Add(new Order { Id = 1 });
            var error = synchronous
                ? await Assert.That(() => db.SaveChanges()).Throws<InvalidOperationException>()
                : await Assert.That(async () => await db.SaveChangesAsync()).Throws<InvalidOperationException>();
            await Assert.That(error!.Message).Contains("Relayer outbox");
            await Assert.That(error.Message).Contains("explicit transaction");
        }
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(0L);
        var attempts = 0;
        var strategy = db.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(async () =>
        {
            await using var attemptScope = provider.CreateAsyncScope();
            var attemptDb = attemptScope.ServiceProvider.GetRequiredService<OrdersContext>();
            var sender = attemptScope.ServiceProvider.GetRequiredService<ISender>();
            await using var transaction = synchronous ? attemptDb.Database.BeginTransaction() : await attemptDb.Database.BeginTransactionAsync();
            await sender.SendAsync(new Probe(2));
            attemptDb.Add(new Order { Id = 2 });
            if (synchronous) attemptDb.SaveChanges(); else await attemptDb.SaveChangesAsync();
            if (++attempts == 1) throw new TimeoutException("Retry this complete unit after rollback.");
            if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
        });
        await Assert.That(attempts).IsEqualTo(2);
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM orders")).IsEqualTo(1L);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    private static List<KeyValuePair<string, string>> DecodeHeaders(byte[] data)
    {
        KeyValuePair<string, WireString>[] entries = [];
        var headers = OutboxHeaderCodec.Read(data, ref entries);
        var result = new List<KeyValuePair<string, string>>(headers.Length);
        foreach (var header in headers.Span) result.Add(new(header.Key, header.Value.ToString()));
        return result;
    }

    private sealed class Database : IAsyncDisposable
    {
        internal static EntityFrameworkCoreOutboxOptions Options => new() { Schema = "Case\"Schema", TablePrefix = "test_" };
        internal const string Table = "\"Case\"\"Schema\".\"test_outbox\"";
        private readonly PostgreSqlFixture _fixture = new();
        internal string ConnectionString => _fixture.ConnectionString;
        internal async Task Start()
        {
            await _fixture.StartAsync();
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            var sql = PostgreSqlOutboxSchema.CreateScript(new() { Schema = Options.Schema, TablePrefix = Options.TablePrefix }) + $"""
                CREATE TABLE orders ("Id" integer PRIMARY KEY CHECK ("Id" > 0));
                CREATE TABLE batch_log (id integer);
                CREATE FUNCTION log_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO batch_log VALUES (1); RETURN NULL; END; $$;
                CREATE TRIGGER batch_count AFTER INSERT ON {Table} FOR EACH STATEMENT EXECUTE FUNCTION log_batch();
                """;
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }
        internal ServiceProvider Services(bool retries = false, NpgsqlDataSource? schedulerSource = null)
        {
            var services = new ServiceCollection();
            if (schedulerSource is not null)
                services.AddRelayerPostgreSqlOrderedDispatch("schedule", schedulerSource,
                    new PostgreSqlOrderedDispatchOptions { Schema = Options.Schema, TablePrefix = Options.TablePrefix, Lanes = Options.Lanes });
            services.AddSingleton<Transport>();
            services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<Transport>());
            services.AddSingleton<IMessageSerializer, Serializer>();
            services.AddRelayerEntityFrameworkCoreOutbox();
            services.AddDbContext<OrdersContext>((sp, o) => o.UseNpgsql(ConnectionString, native =>
            {
                if (retries) native.EnableRetryOnFailure(1, TimeSpan.Zero, null);
            }).AddRelayerOutbox(sp, Options));
            services.AddDbContext<AuditContext>((sp, o) => o.UseNpgsql(ConnectionString).AddRelayerOutbox(sp, Options));
            services.AddRelayer(builder =>
            {
                builder.MessageTypes.Add<Probe>("outbox.probe");
                builder.AddTransport<Empty, Empty, Empty>("events", transport => transport.Send<Probe>("destination"));
            });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }
        internal Task<long> Count() => Scalar($"SELECT count(*) FROM {Table}");
        internal async Task<long> Scalar(string sql)
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            return (long)(await command.ExecuteScalarAsync())!;
        }
        internal async Task VerifyEnvelope(DateTimeOffset scheduled)
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand($"SELECT id, message_type, headers, body, ordering_key, lane, deliver_at FROM {Table} WHERE ordering_key IS NOT NULL", connection);
            await using var reader = await command.ExecuteReaderAsync();
            await reader.ReadAsync();
            var id = reader.GetGuid(0);
            await Assert.That(id.Version).IsEqualTo(7);
            await Assert.That(reader.GetString(1)).IsEqualTo("outbox.probe;v=1");
            var headers = DecodeHeaders(reader.GetFieldValue<byte[]>(2));
            await Assert.That(headers.Single(x => x.Key == "ce_id").Value).IsEqualTo(id.ToString("D"));
            await Assert.That(headers.Single(x => x.Key == "custom").Value).IsEqualTo("é");
            await Assert.That(reader.GetFieldValue<byte[]>(3)[0]).IsEqualTo((byte)1);
            await Assert.That(reader.GetString(4)).IsEqualTo("café");
            await Assert.That(reader.GetInt32(5) is >= 0 and < 32).IsTrue();
            await Assert.That(reader.GetFieldValue<DateTimeOffset>(6)).IsEqualTo(scheduled.AddTicks(-(scheduled.Ticks % 10)));
        }
        public ValueTask DisposeAsync() => _fixture.DisposeAsync();
    }

    public sealed class OrdersContext(DbContextOptions<OrdersContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<Order>().ToTable("orders").HasKey(x => x.Id);
    }
    public sealed class AuditContext(DbContextOptions<AuditContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<Order>().ToTable("orders").HasKey(x => x.Id);
    }
    public sealed class Order { public int Id { get; set; } }
    [Message("outbox.probe")]
    public sealed record Probe(byte Value);
    public sealed class Empty;
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        { destination.GetSpan(1)[0] = ((Probe)(object)message!).Value; destination.Advance(1); }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }
    private sealed class Transport : ITransport
    {
        public int Sends { get; private set; }
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Sends++; return default; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
