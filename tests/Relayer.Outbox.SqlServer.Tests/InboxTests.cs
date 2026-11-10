using System.Buffers;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Configuration;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;
using Relayer.TestFixtures;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer.Tests;

[InheritsTests]
[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public class InboxTests(SqlServerFixture server) : InboxContractTests
{
    protected override async Task<InboxFixture> CreateFixtureAsync()
    {
        var database = new Database(server.ConnectionString);
        await database.Start();
        return database;
    }

    [Test]
    public async Task Inbox_is_opt_in()
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        await using var consumer = new Consumer(database, "orders", inbox: false);
        var id = Guid.CreateVersion7();
        await consumer.DeliverAsync(id);
        await consumer.DeliverAsync(id);
        await Assert.That(consumer.Invocations).IsEqualTo(2);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(2, 0, 2));
    }

    [Test]
    public async Task Inbox_deduplicates_when_connections_inherit_nocount()
    {
        // Server user options affect every new session: use an owned server, isolated from parallel tests.
        await using var isolated = new SqlServerFixture();
        await isolated.InitializeAsync();
        await using var database = new Database(isolated.ConnectionString);
        await database.Start();
        await database.Execute("EXEC sp_configure 'user options', 512; RECONFIGURE");
        using (var pool = new SqlConnection(isolated.ConnectionString)) SqlConnection.ClearPool(pool);
        await using (var connection = new SqlConnection(isolated.ConnectionString))
        {
            await connection.OpenAsync();
            await Assert.That(await connection.ExecuteScalarAsync<int>("SELECT @@OPTIONS & 512")).IsEqualTo(512);
        }
        await using var consumer = new Consumer(database, "orders");
        var id = Guid.CreateVersion7();
        await consumer.DeliverAsync(id);
        await consumer.DeliverAsync(id);
        await Assert.That(consumer.Invocations).IsEqualTo(1);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    public async Task Consumer_identity_is_case_sensitive()
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        await using var lower = new Consumer(database, "orders");
        await using var upper = new Consumer(database, "Orders");
        var id = Guid.CreateVersion7();
        await lower.DeliverAsync(id);
        await upper.DeliverAsync(id);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(2, 2, 2));
    }

    [Test]
    [Arguments("")]
    [Arguments("not-a-uuid")]
    [Arguments("00000000-0000-0000-0000-000000000000")]
    [Arguments("00000000-0000-4000-8000-000000000001")]
    public async Task Inbox_rejects_missing_or_nonversion7_identity(string id)
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        await using var consumer = new Consumer(database, "orders");
        if (id.Length == 0)
            await Assert.That(async () => await consumer.DeliverRawAsync(id)).Throws<ArgumentException>();
        else
            await Assert.That(async () => await consumer.DeliverRawAsync(id)).Throws<MessageEnvelopeException>();
        await Assert.That(consumer.Invocations).IsEqualTo(0);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
    }

    [Test]
    public async Task Cancellation_while_waiting_for_duplicate_lock_leaves_owner_commit_intact()
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        await using var first = new Consumer(database, "orders");
        await using var second = new Consumer(database, "orders");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        first.AfterSave = async _ => { entered.SetResult(); await release.Task.WaitAsync(deadline.Token); };
        var id = Guid.CreateVersion7();
        var owner = first.DeliverAsync(id, deadline.Token);
        try
        {
            await entered.Task.WaitAsync(deadline.Token);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
            var blocked = second.DeliverAsync(id, cancellation.Token);
            await database.WaitForContentionAsync();
            cancellation.Cancel();
            var cancelled = false;
            try { await blocked; }
            catch (OperationCanceledException) { cancelled = true; }
            catch (SqlException error) when (cancellation.IsCancellationRequested &&
                error.Message.Contains("Operation cancelled by user", StringComparison.Ordinal))
            {
                // SqlClient can return its native server-attention error for cancellation during a lock wait.
                cancelled = true;
            }
            await Assert.That(cancelled).IsTrue();
            await Assert.That(second.Invocations).IsEqualTo(0);
        }
        finally
        {
            release.TrySetResult();
            await owner;
        }
        await second.DeliverAsync(id);
        await Assert.That(second.Invocations).IsEqualTo(0);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    public async Task Changed_schema_options_fail_before_beginning_delivery()
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["TablePrefix"] = database.Prefix
        }).Build();
        await using var consumer = new Consumer(database, "orders", configuration: configuration);
        configuration["TablePrefix"] = "changed_";
        configuration.Reload();
        await Assert.That(async () => await consumer.DeliverAsync(Guid.CreateVersion7())).Throws<InvalidOperationException>();
        await Assert.That(consumer.Invocations).IsEqualTo(0);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
    }

    [Test]
    public async Task Registration_rejects_ambiguous_consumer_names_and_invalid_native_configuration()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            new MessageTypeRegistry(), new Serializer(), static (_, error, _) => ValueTask.FromException(error));
        foreach (var name in new[] { " ", "orders ", new string('x', 257) })
            await Assert.That(() => builder.SqlServer(name, "Server=unused")).Throws<ArgumentException>();
        await Assert.That(() => builder.SqlServer("orders", "NotANativeKeyword=value")).Throws<ArgumentException>();
        await Assert.That(() => builder.SqlServer("orders", "Server=unused")).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments("commit")]
    [Arguments("commitAsync")]
    [Arguments("rollback")]
    [Arguments("rollbackAsync")]
    [Arguments("dispose")]
    [Arguments("disposeAsync")]
    public async Task Handler_cannot_complete_consumer_transaction(string operation)
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        await using var consumer = new Consumer(database, "orders");
        consumer.AfterSave = async context =>
        {
            var transaction = context.Services.GetRequiredService<SqlServerOutbox>().CurrentTransaction!;
            switch (operation)
            {
                case "commit": transaction.Commit(); break;
                case "commitAsync": await transaction.CommitAsync(); break;
                case "rollback": transaction.Rollback(); break;
                case "rollbackAsync": await transaction.RollbackAsync(); break;
                case "dispose": transaction.Dispose(); break;
                case "disposeAsync": await transaction.DisposeAsync(); break;
            }
        };
        await Assert.That(async () => await consumer.DeliverAsync(Guid.CreateVersion7())).Throws<InvalidOperationException>();
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Scope_disposal_cannot_change_committed_outcome(bool duplicate)
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        await using var consumer = new Consumer(database, "orders");
        var id = Guid.CreateVersion7();
        if (duplicate) await consumer.DeliverAsync(id);
        consumer.Control.OnDispose = () =>
        {
            // A committed Complete disposition is immutable even before broker settlement.
            var settlement = consumer.Control.Context!.Features.GetRequired<ISettlementFeature>();
            Assert.Throws<InvalidOperationException>(() => settlement.Retry(TimeSpan.Zero));
        };
        // Duplicate handling skips the handler, so capture the context in middleware too.
        await consumer.DeliverAsync(id);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    public async Task Outbox_insert_failure_rolls_back_business_and_inbox()
    {
        await using var database = new Database(server.ConnectionString);
        await database.Start();
        await database.Execute($"ALTER TABLE {database.Table("outbox")} ADD CONSTRAINT [reject_{database.Prefix}] CHECK (attempts < 0)");
        await using var consumer = new Consumer(database, "orders");
        await Assert.That(async () => await consumer.DeliverAsync(Guid.CreateVersion7())).Throws<SqlException>();
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
    }

    private sealed class Database(string connectionString) : InboxFixture
    {
        internal string Prefix { get; } = "inbox_" + Guid.NewGuid().ToString("N") + "_";
        internal string ConnectionString => connectionString;
        internal SqlServerOutboxOptions Options => new() { TablePrefix = Prefix };
        internal string Table(string name) => $"[dbo].[{Prefix}{name}]";
        internal Task Start() => Execute(SqlServerOutboxSchema.CreateScript(Options) + $"CREATE TABLE {Table("business")} (id int NOT NULL)");
        internal async Task Execute(string sql)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await connection.ExecuteAsync(sql);
        }
        public override InboxConsumer Consumer(string name = "orders") => new Consumer(this, name);
        public override async Task<DurableState> ReadStateAsync()
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return new(await connection.ExecuteScalarAsync<long>($"SELECT COUNT_BIG(*) FROM {Table("business")} WITH (READPAST)"),
                await connection.ExecuteScalarAsync<long>($"SELECT COUNT_BIG(*) FROM {Table("inbox")} WITH (READPAST)"),
                await connection.ExecuteScalarAsync<long>($"SELECT COUNT_BIG(*) FROM {Table("outbox")} WITH (READPAST)"));
        }
        public override async Task WaitForContentionAsync()
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(deadline.Token);
            while (true)
            {
                var command = new CommandDefinition("SELECT COUNT_BIG(*) FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t WHERE r.wait_type LIKE 'LCK%' AND t.text LIKE @pattern",
                    new { pattern = "%" + Prefix + "inbox%" }, cancellationToken: deadline.Token);
                if (await connection.ExecuteScalarAsync<long>(command) > 0) return;
                await Task.Delay(10, deadline.Token);
            }
        }
        public override async ValueTask DisposeAsync() => await Execute($"DROP TABLE {Table("business")}, {Table("inbox")}, {Table("outbox")}");
    }

    private sealed class Consumer : InboxConsumer
    {
        private readonly ServiceProvider _provider;
        private readonly ConsumePipeline _pipeline;
        internal Control Control { get; } = new();
        internal Consumer(Database database, string name, bool inbox = true, IConfiguration? configuration = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton(Control);
            services.AddScoped<Disposer>();
            services.AddSingleton<Transport>();
            services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<Transport>());
            services.AddSingleton<IMessageSerializer, Serializer>();
            if (configuration is null) services.AddRelayerSqlServerOutbox(database.Options);
            else services.AddRelayerSqlServerOutbox(configuration);
            services.AddScoped<SqlServerOutbox>(sp =>
            {
                _ = sp.GetRequiredService<Disposer>();
                return new(sp.GetRequiredService<IOptionsSnapshot<SqlServerOutboxOptions>>(), sp.GetRequiredService<RelayerInstrumentation>());
            });
            services.AddRelayer(r =>
            {
                r.MessageTypes.Add<Probe>("inbox.probe");
                r.Handler<Handler>();
                r.AddTransport<Empty, Empty, Empty>("events", t => t.Send<Probe>("destination"));
            });
            _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
            Control.Business = database.Table("business");
            Control.AfterSave = context => AfterSave?.Invoke(context) ?? default;
            _pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
                _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
                .SqlServer(name, database.ConnectionString, inbox)
                .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
                .Settle(_ => Settlement?.Invoke() ?? default).Build();
        }
        public override RelayerInstrumentation Instrumentation => _provider.GetRequiredService<RelayerInstrumentation>();
        public override int Invocations => Control.Invocations;
        public override int DirectSends => _provider.GetRequiredService<Transport>().Sends;
        public override Task DeliverAsync(Guid messageId, CancellationToken cancellation = default) => DeliverRawAsync(messageId.ToString("D"), cancellation);
        internal async Task DeliverRawAsync(string messageId, CancellationToken cancellation = default)
        {
            using var context = _pipeline.RentContext(new ConsumeMessage
            {
                MessageType = new("inbox.probe", 1), MessageId = messageId,
                Body = new ReadOnlySequence<byte>(new byte[] { 1 })
            }, cancellation);
            Control.Context = context;
            await _pipeline.ProcessAsync(context);
        }
        public override ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    public sealed class Control
    {
        public int Invocations;
        public string Business = "";
        public Func<MessageContext, ValueTask>? AfterSave;
        public Action? OnDispose;
        public MessageContext? Context;
    }
    public sealed class Disposer(Control control) : IDisposable, IAsyncDisposable
    {
        public void Dispose() => control.OnDispose?.Invoke();
        public ValueTask DisposeAsync() { Dispose(); return default; }
    }
    public sealed class Handler(SqlServerOutbox outbox, ISender sender, Control control) : IHandler<Probe>
    {
        public async ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            var invocation = Interlocked.Increment(ref control.Invocations);
            var transaction = outbox.CurrentTransaction!;
            await transaction.Connection!.ExecuteAsync(new CommandDefinition($"INSERT INTO {control.Business} VALUES (@id)",
                new { id = invocation }, transaction, cancellationToken: cancellationToken));
            await sender.SendAsync(new Probe(invocation), cancellationToken);
            if (control.AfterSave is { } after) await after(context);
        }
    }
    [Message("inbox.probe")]
    public sealed record Probe(int Value);
    public sealed class Empty;
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        { destination.GetSpan(1)[0] = 1; destination.Advance(1); }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)new Probe(1);
    }
    private sealed class Transport : ITransport
    {
        public int Sends;
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { DeadLetter = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Interlocked.Increment(ref Sends); return default; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
