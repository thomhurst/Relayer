using System.Buffers;
using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;
using Relayer.Serialization;
using Relayer.TestFixtures;
using Relayer.Transports;

namespace Relayer.Outbox.MySql.Tests;

[InheritsTests]
public class InboxTests : InboxContractTests
{
    protected override async Task<InboxFixture> CreateFixtureAsync()
    {
        var database = new Database();
        try { await database.Start(); return database; }
        catch { await database.DisposeAsync(); throw; }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Inbox_is_opt_in(bool inbox)
    {
        await using var database = new Database();
        await database.Start();
        await using var consumer = new Consumer(database, "orders", inbox);
        var id = Guid.CreateVersion7();
        await consumer.DeliverAsync(id);
        await consumer.DeliverAsync(id);
        await Assert.That(consumer.Invocations).IsEqualTo(inbox ? 1 : 2);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(inbox ? 1 : 2, inbox ? 1 : 0, inbox ? 1 : 2));
    }

    [Test]
    [Arguments("commit")]
    [Arguments("commit-async")]
    [Arguments("rollback")]
    [Arguments("rollback-async")]
    [Arguments("dispose")]
    [Arguments("dispose-async")]
    public async Task Handler_cannot_complete_the_owned_transaction(string action)
    {
        await using var database = new Database();
        await database.Start();
        await using var consumer = new Consumer(database, "orders");
        consumer.AfterSave = async context =>
        {
            var transaction = context.Services.GetRequiredService<MySqlOutbox>().CurrentTransaction!;
            switch (action)
            {
                case "commit": transaction.Commit(); break;
                case "commit-async": await transaction.CommitAsync(); break;
                case "rollback": transaction.Rollback(); break;
                case "rollback-async": await transaction.RollbackAsync(); break;
                case "dispose": transaction.Dispose(); break;
                case "dispose-async": await transaction.DisposeAsync(); break;
            }
        };
        await Assert.That(async () => await consumer.DeliverAsync(Guid.CreateVersion7())).Throws<InvalidOperationException>();
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
    }

    [Test]
    [Arguments("bad")]
    [Arguments("00000000-0000-0000-0000-000000000000")]
    [Arguments("00000000-0000-4000-8000-000000000001")]
    public async Task Invalid_identity_fails_before_opening_database(string id)
    {
        await using var source = new MySqlDataSource("Server=unused");
        await using var provider = Services();
        var middleware = new MySqlConsumer(source, "orders", "orders"u8.ToArray(), true,
            System.Data.IsolationLevel.Unspecified, provider.GetRequiredService<RelayerInstrumentation>());
        await Assert.That(async () => await middleware.InvokeAsync(new Context(provider, id), static _ => throw new InvalidOperationException("Handler ran")))
            .Throws<MessageEnvelopeException>();
    }

    [Test]
    public async Task Outbox_failure_rolls_back_inbox_and_business_work()
    {
        await using var database = new Database();
        await database.Start();
        await database.Fixture.Execute("ALTER TABLE relayer_outbox ADD CONSTRAINT reject_rows CHECK (attempts < 0)");
        await using var consumer = new Consumer(database, "orders");
        var id = Guid.CreateVersion7();
        await Assert.That(async () => await consumer.DeliverAsync(id)).Throws<MySqlException>();
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(0, 0, 0));
        await database.Fixture.Execute("ALTER TABLE relayer_outbox DROP CHECK reject_rows");
        await consumer.DeliverAsync(id);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Scope_disposal_cannot_change_committed_outcome(bool duplicate)
    {
        await using var database = new Database();
        await database.Start();
        await using var consumer = new Consumer(database, "orders");
        var id = Guid.CreateVersion7();
        if (duplicate) await consumer.DeliverAsync(id);
        consumer.Control.OnDispose = context => context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.Zero);
        await Assert.That(async () => await consumer.DeliverAsync(id)).Throws<InvalidOperationException>();
        consumer.Control.OnDispose = null;
        await consumer.DeliverAsync(id);
        await Assert.That(consumer.Invocations).IsEqualTo(1);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Exact_consumer_identity_and_native_affected_rows_are_preserved(bool affectedRows)
    {
        await using var database = new Database();
        await database.Start();
        await using var source = new MySqlDataSource(new MySqlConnectionStringBuilder(database.Fixture.Source.ConnectionString)
            { UseAffectedRows = affectedRows }.ConnectionString);
        var id = Guid.CreateVersion7();
        foreach (var name in new[] { "orders", "Orders", "orders ", "é" })
        {
            await using var consumer = new Consumer(database, name, source: source);
            await consumer.DeliverAsync(id);
            await consumer.DeliverAsync(id);
            await Assert.That(consumer.Invocations).IsEqualTo(1);
        }
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(4, 4, 4));
    }

    [Test]
    public async Task Immediate_failure_retry_uses_a_fresh_scope()
    {
        await using var database = new Database();
        await database.Start();
        await using var consumer = new Consumer(database, "orders", retry: true);
        var scopes = new List<IServiceProvider>();
        consumer.AfterSave = context =>
        {
            scopes.Add(context.Services);
            return context.Attempt == 1 ? ValueTask.FromException(new InvalidOperationException("Retry the unit")) : default;
        };
        var id = Guid.CreateVersion7();
        await consumer.DeliverAsync(id);
        await consumer.DeliverAsync(id);
        await Assert.That(scopes.Count).IsEqualTo(2);
        await Assert.That(scopes[0]).IsNotSameReferenceAs(scopes[1]);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    public async Task Cancellation_while_waiting_for_duplicate_does_not_settle_or_repeat_work()
    {
        await using var database = new Database();
        await database.Start();
        await using var first = new Consumer(database, "orders");
        await using var second = new Consumer(database, "orders");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var cancellation = new CancellationTokenSource();
        first.AfterSave = async _ => { entered.SetResult(); await release.Task.WaitAsync(deadline.Token); };
        var settled = 0;
        second.Settlement = () => { settled++; return default; };
        var id = Guid.CreateVersion7();
        var initial = first.DeliverAsync(id, deadline.Token);
        Task? waiting = null;
        try
        {
            await entered.Task.WaitAsync(deadline.Token);
            waiting = second.DeliverAsync(id, cancellation.Token);
            await database.WaitForContentionAsync();
            cancellation.Cancel();
            await Assert.That(async () => await waiting).Throws<OperationCanceledException>();
            await Assert.That(settled).IsEqualTo(0);
            await Assert.That(second.Invocations).IsEqualTo(0);
        }
        finally
        {
            cancellation.Cancel();
            release.TrySetResult();
            await initial;
            if (waiting is not null) try { await waiting; } catch (OperationCanceledException) { }
        }
        await second.DeliverAsync(id);
        await Assert.That(await database.ReadStateAsync()).IsEqualTo(new DurableState(1, 1, 1));
    }

    [Test]
    [Arguments("")]
    [Arguments(" ")]
    [Arguments("\ud800")]
    public async Task Invalid_consumer_names_fail_during_composition(string name)
    {
        await using var source = new MySqlDataSource("Server=unused");
        await using var provider = Services();
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error));
        await Assert.That(() => builder.MySql(source, name, inbox: true)).Throws<ArgumentException>();
        await Assert.That(() => builder.MySql(source, new string('a', 769), inbox: true)).Throws<ArgumentException>();
    }

    private static ServiceProvider Services(Control? control = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(control ?? new Control());
        services.AddSingleton<Transport>();
        services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<Transport>());
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddRelayerMySqlOutbox();
        services.AddScoped<ScopeObserver>();
        services.AddScoped<MySqlOutbox>(sp =>
        {
            _ = sp.GetRequiredService<ScopeObserver>();
            return new(sp.GetRequiredService<Microsoft.Extensions.Options.IOptionsSnapshot<MySqlOutboxOptions>>(),
                sp.GetRequiredService<RelayerInstrumentation>());
        });
        services.AddRelayer(r =>
        {
            r.MessageTypes.Add<Probe>("inbox.probe");
            r.Handler<Handler>();
            r.AddTransport<Empty, Empty, Empty>("events", t => t.Send<Probe>("destination"));
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private sealed class Database : InboxFixture
    {
        internal MySqlFixture Fixture { get; } = new();
        internal async Task Start()
        {
            await Fixture.StartAsync();
            await Fixture.Execute("CREATE TABLE orders (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB");
        }
        public override InboxConsumer Consumer(string name = "orders") => new Consumer(this, name);
        public override async Task<DurableState> ReadStateAsync() =>
            new(await Count("orders"), await Count("relayer_inbox"), await Count("relayer_outbox"));
        private async Task<long> Count(string table)
        {
            await using var command = Fixture.Source.CreateCommand($"SELECT COUNT(*) FROM `{table}`");
            return (long)(await command.ExecuteScalarAsync())!;
        }
        public override async Task WaitForContentionAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            while (true)
            {
                await using var command = Fixture.Source.CreateCommand("SELECT COUNT(*) FROM performance_schema.data_lock_waits");
                if (Convert.ToInt64(await command.ExecuteScalarAsync(timeout.Token)) > 0) return;
                await Task.Delay(10, timeout.Token);
            }
        }
        public override ValueTask DisposeAsync() => Fixture.DisposeAsync();
    }

    private sealed class Consumer : InboxConsumer
    {
        private readonly ServiceProvider _provider;
        private readonly ConsumePipeline _pipeline;
        internal Control Control { get; } = new();
        public Consumer(Database database, string name, bool inbox = true, MySqlDataSource? source = null, bool retry = false)
        {
            _provider = Services(Control);
            Control.AfterSave = context => AfterSave?.Invoke(context) ?? default;
            var scopes = _provider.GetRequiredService<IServiceScopeFactory>();
            var messages = _provider.GetRequiredService<MessageTypeRegistry>();
            var builder = retry
                ? new ConsumePipelineBuilder(scopes, messages, new Serializer(), new FailurePipeline(_provider, "events", "source", name,
                    new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 }))
                : new ConsumePipelineBuilder(scopes, messages, new Serializer(), static (_, error, _) => ValueTask.FromException(error));
            _pipeline = builder.Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
                .MySql(source ?? database.Fixture.Source, name, inbox)
                .Settle(_ => Settlement?.Invoke() ?? default).Build();
        }
        public override RelayerInstrumentation Instrumentation => _provider.GetRequiredService<RelayerInstrumentation>();
        public override int Invocations => Control.Invocations;
        public override int DirectSends => _provider.GetRequiredService<Transport>().Sends;
        public override async Task DeliverAsync(Guid messageId, CancellationToken cancellation = default)
        {
            using var context = _pipeline.RentContext(new ConsumeMessage
            {
                MessageType = new("inbox.probe", 1), MessageId = messageId.ToString("D"), Body = new ReadOnlySequence<byte>(new byte[] { 1 })
            }, cancellation);
            // Resolve the observer even on duplicate paths so disposal ordering is exercised.
            Control.Context = context;
            await _pipeline.ProcessAsync(context);
        }
        public override ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    public sealed class Control
    {
        public int Invocations;
        public MessageContext? Context;
        public Func<MessageContext, ValueTask>? AfterSave;
        public Action<MessageContext>? OnDispose;
    }
    public sealed class ScopeObserver(Control control) : IDisposable
    {
        public void Dispose() { if (control.Context is { } context) control.OnDispose?.Invoke(context); }
    }
    public sealed class Handler(MySqlOutbox outbox, ISender sender, Control control, ScopeObserver observer) : IHandler<Probe>
    {
        public async ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            _ = observer;
            Interlocked.Increment(ref control.Invocations);
            var transaction = outbox.CurrentTransaction!;
            await using var command = transaction.Connection!.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO orders VALUES ()";
            await command.ExecuteNonQueryAsync(cancellationToken);
            await sender.SendAsync(new Probe(1), cancellationToken);
            if (control.AfterSave is { } after) await after(context);
        }
    }
    [Message("inbox.probe")]
    public sealed record Probe(int Value);
    public sealed class Empty;
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) { destination.GetSpan(1)[0] = 1; destination.Advance(1); }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)new Probe(1);
    }
    private sealed class Context(IServiceProvider services, string id) : MessageContext
    {
        public override string MessageId => id;
        public override MessageType MessageType => new("inbox.probe", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => services;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
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
