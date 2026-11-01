using System.Buffers;
using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class InboxTests
{
    [Test]
    public async Task Redelivery_skips_handler_and_records_metric_after_atomic_commit()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true);
        var control = provider.GetRequiredService<Control>();
        control.AfterSave = async context =>
        {
            await Assert.That(context.Services.GetRequiredService<OrdersContext>().Database.CurrentTransaction).IsNotNull();
            await Assert.That(await database.Orders()).IsEqualTo(0L);
            await Assert.That(await database.Count("inbox")).IsEqualTo(0L);
            await Assert.That(await database.Count("outbox")).IsEqualTo(0L);
            context.Features.GetRequired<ISettlementFeature>().Complete();
        };
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        long duplicates = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meter) =>
        {
            if (instrument.Name == "relayer.inbox.duplicates" && ReferenceEquals(instrument.Meter.Scope, instrumentation))
                meter.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, count, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "relayer.consumer" && Equals(tag.Value, "orders")) Interlocked.Add(ref duplicates, count);
        });
        listener.Start();
        var settlements = 0;
        var pipeline = Pipeline(provider).Settle(async _ =>
        {
            await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
            await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
            await Assert.That(await database.Orders()).IsEqualTo(1L);
            settlements++;
        }).Build();
        var message = Message();
        await Deliver(pipeline, message);
        await Deliver(pipeline, message);
        await Assert.That(control.Invocations).IsEqualTo(1);
        await Assert.That(duplicates).IsEqualTo(1L);
        await Assert.That(settlements).IsEqualTo(2);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    [Arguments(false, false, 2)]
    [Arguments(false, true, 1)]
    [Arguments(true, false, 1)]
    public async Task Inbox_is_opt_in_without_outbox_and_automatic_with_same_context(bool outbox, bool inbox, int calls)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox);
        var control = provider.GetRequiredService<Control>();
        var pipeline = Pipeline(provider, inbox: inbox).Build();
        var message = Message();
        await Deliver(pipeline, message);
        await Deliver(pipeline, message);
        await Assert.That(control.Invocations).IsEqualTo(calls);
        await Assert.That(await database.Count("inbox")).IsEqualTo(calls == 1 ? 1L : 0L);
    }

    [Test]
    public async Task Identity_is_scoped_by_consumer_and_other_context_outbox_does_not_enable_inbox()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: false);
        var message = Message();
        await Deliver(Pipeline(provider, "first", inbox: true).Build(), message);
        await Deliver(Pipeline(provider, "second", inbox: true).Build(), message);
        await Assert.That(await database.Count("inbox")).IsEqualTo(2L);
        await Assert.That(provider.GetRequiredService<Control>().Invocations).IsEqualTo(2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Concurrent_redelivery_waits_for_commit_or_runs_after_rollback(bool rollback)
    {
        await using var database = new Database();
        await database.Start();
        await using var firstProvider = database.Services(outbox: true);
        await using var secondProvider = database.Services(outbox: true);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        firstProvider.GetRequiredService<Control>().AfterSave = async _ =>
        {
            entered.SetResult();
            await release.Task;
            if (rollback) throw new InvalidOperationException("Roll back first delivery.");
        };
        var message = Message();
        var first = Deliver(Pipeline(firstProvider).Build(), message);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(20));
        var second = Deliver(Pipeline(secondProvider).Build(), message);
        try
        {
            // Observe PostgreSQL's lock wait, rather than guessing from elapsed time or scheduling.
            await database.WaitForInboxLock();
            await Assert.That(secondProvider.GetRequiredService<Control>().Invocations).IsEqualTo(0);
        }
        finally { release.TrySetResult(); }
        if (rollback) await Assert.That(async () => await first).Throws<InvalidOperationException>();
        else await first;
        await second;
        await Assert.That(secondProvider.GetRequiredService<Control>().Invocations).IsEqualTo(rollback ? 1 : 0);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
    }

    [Test]
    [Arguments("handler")]
    [Arguments("cancel")]
    [Arguments("outbox")]
    [Arguments("retry")]
    [Arguments("deadletter")]
    public async Task Failed_or_noncomplete_work_rolls_back_and_redelivery_runs_handler(string failure)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true);
        var control = provider.GetRequiredService<Control>();
        using var cancellation = new CancellationTokenSource();
        control.AfterSave = context =>
        {
            switch (failure)
            {
                case "handler": throw new InvalidOperationException("Handler failed after save.");
                case "cancel": cancellation.Cancel(); break;
                case "retry": context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.Zero); break;
                case "deadletter": context.Features.GetRequired<ISettlementFeature>().DeadLetter("Rejected"); break;
            }
            return default;
        };
        if (failure == "outbox")
            await database.Execute("ALTER TABLE \"Case\"\"Inbox\".\"test_outbox\" ADD CONSTRAINT reject_rows CHECK (false)");
        var settled = 0;
        var pipeline = Pipeline(provider).Settle(_ => { settled++; return default; }).Build();
        var message = Message();
        if (failure is "retry" or "deadletter") await Deliver(pipeline, message);
        else if (failure == "cancel")
            await Assert.That(async () => await Deliver(pipeline, message, cancellation.Token)).Throws<OperationCanceledException>();
        else if (failure == "outbox")
            await Assert.That(async () => await Deliver(pipeline, message)).Throws<PostgresException>();
        else await Assert.That(async () => await Deliver(pipeline, message)).Throws<InvalidOperationException>();
        await Assert.That(settled).IsEqualTo(failure is "retry" or "deadletter" ? 1 : 0);
        await Assert.That(await database.Count("inbox")).IsEqualTo(0L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(0L);
        await Assert.That(await database.Orders()).IsEqualTo(0L);
        if (failure == "outbox") await database.Execute("ALTER TABLE \"Case\"\"Inbox\".\"test_outbox\" DROP CONSTRAINT reject_rows");
        control.AfterSave = null;
        await Deliver(pipeline, message);
        await Assert.That(control.Invocations).IsEqualTo(2);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
    }

    [Test]
    public async Task Settlement_failure_after_commit_redelivers_without_repeating_work()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true);
        var calls = 0;
        var pipeline = Pipeline(provider).Settle(_ => ++calls == 1
            ? ValueTask.FromException(new InvalidOperationException("Lost acknowledgement.")) : default).Build();
        var message = Message();
        await Assert.That(async () => await Deliver(pipeline, message)).Throws<InvalidOperationException>();
        await Deliver(pipeline, message);
        await Assert.That(provider.GetRequiredService<Control>().Invocations).IsEqualTo(1);
        await Assert.That(calls).IsEqualTo(2);
    }

    [Test]
    [Arguments(false, false, "retry")]
    [Arguments(false, false, "deadletter")]
    [Arguments(false, true, "retry")]
    [Arguments(false, true, "deadletter")]
    [Arguments(true, false, "retry")]
    [Arguments(true, false, "deadletter")]
    [Arguments(true, true, "retry")]
    [Arguments(true, true, "deadletter")]
    public async Task Settlement_requests_during_disposal_cannot_change_a_committed_outcome(bool failures, bool duplicate, string request)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true);
        var control = provider.GetRequiredService<Control>();
        var failurePipeline = failures ? new FailurePipeline(provider, "events", "source", "orders", new()) : null;
        var settled = 0;
        var pipeline = Pipeline(provider, failures: failurePipeline).Settle(_ => { settled++; return default; }).Build();
        var message = Message();
        if (duplicate) await Deliver(pipeline, message);
        using var context = pipeline.RentContext(message);
        control.OnDispose = () =>
        {
            var settlement = context.Features.GetRequired<ISettlementFeature>();
            if (request == "retry") settlement.Retry(TimeSpan.Zero);
            else settlement.DeadLetter("Rejected during disposal");
        };
        await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<InvalidOperationException>()
            .WithMessage("Only one settlement request is allowed per processing attempt.");
        control.OnDispose = null;
        await Assert.That(settled).IsEqualTo(duplicate ? 1 : 0);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
        // A fresh attempt acknowledges the committed duplicate without repeating business work.
        await Deliver(pipeline, message);
        await Assert.That(control.Invocations).IsEqualTo(1);
        await Assert.That(settled).IsEqualTo(duplicate ? 2 : 1);
    }

    [Test]
    public async Task Immediate_retry_uses_a_fresh_scope_and_commits_only_the_successful_attempt()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true);
        var scopes = new List<IServiceProvider>();
        provider.GetRequiredService<Control>().AfterSave = context =>
        {
            scopes.Add(context.Services);
            return context.Attempt == 1 ? ValueTask.FromException(new InvalidOperationException("Retry the entire unit.")) : default;
        };
        var failures = new FailurePipeline(provider, "events", "source", "orders",
            new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 });
        var pipeline = Pipeline(provider, failures: failures).Build();
        var message = Message();
        await Deliver(pipeline, message);
        await Deliver(pipeline, message);
        await Assert.That(scopes.Count).IsEqualTo(2);
        await Assert.That(scopes[0]).IsNotSameReferenceAs(scopes[1]);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    [Arguments("not-a-guid")]
    [Arguments("00000000-0000-0000-0000-000000000000")]
    [Arguments("00000000-0000-4000-8000-000000000001")]
    public async Task Invalid_identity_fails_before_handler_or_database_write(string id)
    {
        await using var provider = new Database().Services(outbox: true);
        var pipeline = Pipeline(provider).Build();
        await Assert.That(async () => await Deliver(pipeline, Message() with { MessageId = id })).Throws<MessageEnvelopeException>();
        await Assert.That(provider.GetRequiredService<Control>().Invocations).IsEqualTo(0);
    }

    [Test]
    public async Task Retrying_strategy_is_rejected_during_composition()
    {
        await using var provider = new Database().Services(outbox: true, retries: true);
        await Assert.That(() => Pipeline(provider)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Inbox_follows_the_outbox_context_configuration_of_each_delivery_scope()
    {
        await using var database = new Database();
        await database.Start();
        var options = Database.Options;
        await using var provider = database.Services(outbox: true, contextOptions: () => options);
        var pipeline = Pipeline(provider).Build();
        options = new() { Schema = Database.Options.Schema, TablePrefix = "reloaded_" };
        await database.Execute(PostgreSqlOutboxSchema.CreateScript(new() { Schema = options.Schema, TablePrefix = options.TablePrefix }));
        var message = Message();
        await Deliver(pipeline, message);
        await Deliver(pipeline, message);
        await Assert.That(await database.Count("inbox")).IsEqualTo(0L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM \"Case\"\"Inbox\".reloaded_inbox")).IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM \"Case\"\"Inbox\".reloaded_outbox")).IsEqualTo(1L);
        await Assert.That(provider.GetRequiredService<Control>().Invocations).IsEqualTo(1);
    }

    private static ConsumePipelineBuilder Pipeline(ServiceProvider provider, string consumer = "orders", bool inbox = false,
        FailurePipeline? failures = null)
    {
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        var messages = provider.GetRequiredService<MessageTypeRegistry>();
        var builder = failures is null
            ? new ConsumePipelineBuilder(scopes, messages, new Serializer(), static (_, error, _) => ValueTask.FromException(error))
            : new ConsumePipelineBuilder(scopes, messages, new Serializer(), failures);
        return builder.Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
            .EntityFrameworkCore<OrdersContext>(consumer, inbox, inbox ? Database.Options : null);
    }

    private static ConsumeMessage Message() => new()
    {
        MessageType = new("inbox.probe", 1), MessageId = Guid.CreateVersion7().ToString("D"), Body = new ReadOnlySequence<byte>(new byte[] { 1 })
    };

    private static async Task Deliver(ConsumePipeline pipeline, ConsumeMessage message, CancellationToken cancellation = default)
    {
        using var context = pipeline.RentContext(message, cancellation);
        await pipeline.ProcessAsync(context);
    }

    public sealed class Control
    {
        public int Invocations;
        public bool Outbox;
        public Func<MessageContext, ValueTask>? AfterSave;
        public Action? OnDispose;
    }

    public sealed class Handler(OrdersContext db, ISender sender, Control control) : IHandler<Probe>
    {
        public async ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref control.Invocations);
            db.Add(new Order { Id = Guid.CreateVersion7() });
            if (control.Outbox) await sender.SendAsync(new Probe(call), cancellationToken);
            await db.SaveChangesAsync(cancellationToken);
            if (control.AfterSave is { } after) await after(context);
        }
    }

    public sealed class OrdersContext(DbContextOptions<OrdersContext> options, Control control) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<Order>().ToTable("orders").HasKey(x => x.Id);
        public override async ValueTask DisposeAsync()
        {
            await base.DisposeAsync();
            control.OnDispose?.Invoke();
        }
    }
    public sealed class OtherContext(DbContextOptions<OtherContext> options) : DbContext(options);
    public sealed class Order { public Guid Id { get; set; } }
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

    private sealed class Database : IAsyncDisposable
    {
        internal static EntityFrameworkCoreOutboxOptions Options => new() { Schema = "Case\"Inbox", TablePrefix = "test_" };
        private readonly PostgreSqlFixture _fixture = new();
        internal async Task Start()
        {
            await _fixture.StartAsync();
            await Execute(PostgreSqlOutboxSchema.CreateScript(new() { Schema = Options.Schema, TablePrefix = Options.TablePrefix }) +
                "CREATE TABLE orders (\"Id\" uuid PRIMARY KEY)");
        }
        internal ServiceProvider Services(bool outbox, bool retries = false, Func<EntityFrameworkCoreOutboxOptions>? contextOptions = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton(new Control { Outbox = outbox });
            services.AddSingleton<Transport>();
            services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<Transport>());
            services.AddSingleton<IMessageSerializer, Serializer>();
            services.AddRelayerEntityFrameworkCoreOutbox();
            services.AddDbContext<OrdersContext>((sp, o) =>
            {
                o.UseNpgsql(string.IsNullOrEmpty(_fixture.ConnectionString) ? "Host=unused" : _fixture.ConnectionString,
                    native => { if (retries) native.EnableRetryOnFailure(); });
                if (outbox) o.AddRelayerOutbox(sp, contextOptions?.Invoke() ?? Options);
            });
            // Outbox on another context must not turn on the inbox for OrdersContext.
            services.AddDbContext<OtherContext>((sp, o) => o.UseNpgsql("Host=unused").AddRelayerOutbox(sp));
            services.AddRelayer(r =>
            {
                r.MessageTypes.Add<Probe>("inbox.probe");
                r.Handler<Handler>();
                r.AddTransport<Empty, Empty, Empty>("events", t => t.Send<Probe>("destination"));
            });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }
        internal Task<long> Count(string table) => Scalar($"SELECT count(*) FROM \"Case\"\"Inbox\".\"test_{table}\"");
        internal Task<long> Orders() => Scalar("SELECT count(*) FROM orders");
        internal async Task<long> Scalar(string sql)
        {
            await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            return (long)(await command.ExecuteScalarAsync())!;
        }
        internal async Task Execute(string sql)
        {
            await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }
        internal async Task WaitForInboxLock()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
            await connection.OpenAsync(timeout.Token);
            while (true)
            {
                await using var command = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO %test_inbox%'", connection);
                if ((long)(await command.ExecuteScalarAsync(timeout.Token))! > 0) return;
                await Task.Delay(10, timeout.Token);
            }
        }
        public ValueTask DisposeAsync() => _fixture.DisposeAsync();
    }
}
