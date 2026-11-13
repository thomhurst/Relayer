using Relayer.Pipeline;
using System.Buffers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Sagas.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

// An executable process-manager call site: ordinary typed messages and scheduled ISender sends.
public class SagaTimeoutTests
{
    [Test]
    public async Task Committed_timeout_survives_provider_restart_dispatches_only_when_due_and_deduplicates()
    {
        await using var database = new Database();
        await database.Start();
        var processId = Guid.NewGuid();
        Guid scheduledId;
        await using (var provider = database.Services())
        {
            var control = provider.GetRequiredService<Control>();
            control.AfterSave = async () =>
            {
                await Assert.That(await database.Count("processes")).IsEqualTo(0L);
                await Assert.That(await database.Count("relayer_outbox")).IsEqualTo(0L);
                await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(0L);
            };
            await Deliver(provider, new BeginWait(processId));
            await Assert.That(await database.Count("relayer_outbox")).IsEqualTo(1L);
            await using var read = database.Source.CreateCommand("SELECT deliver_at FROM relayer_outbox");
            await Assert.That((DateTime)(await read.ExecuteScalarAsync())!).IsEqualTo(control.Clock.GetUtcNow().AddDays(1).UtcDateTime);
            await using var identity = database.Source.CreateCommand("SELECT id FROM relayer_outbox");
            scheduledId = (Guid)(await identity.ExecuteScalarAsync())!;
            await Assert.That(provider.GetRequiredService<Transport>().Messages.Count).IsEqualTo(0);
        }

        // A fresh provider has no in-memory scheduling state from the transition.
        await using var restarted = database.Services();
        var worker = restarted.GetRequiredKeyedService<PostgreSqlMaintenanceWorker>("timeouts");
        try
        {
            await Assert.That(await worker.SweepAsync(default)).IsEqualTo(0);
            await Assert.That(restarted.GetRequiredService<Transport>().Messages.Count).IsEqualTo(0);
            // The worker uses PostgreSQL time. Move durable eligibility explicitly; never sleep for a deadline.
            await database.Execute("UPDATE relayer_outbox SET deliver_at = clock_timestamp() - interval '1 second'");
            await Assert.That(await worker.SweepAsync(default)).IsEqualTo(1);
            var scheduled = restarted.GetRequiredService<Transport>().Messages.Single();
            await Assert.That(Guid.Parse(scheduled.MessageId)).IsEqualTo(scheduledId);
            await Assert.That(scheduled.MessageType).IsEqualTo(new MessageType("timeout.expired", 1));
            await Deliver(restarted, scheduled);
            await Deliver(restarted, scheduled);
            await Assert.That(restarted.GetRequiredService<Control>().TimeoutLoads).IsEqualTo(1);
            await Assert.That(restarted.GetRequiredService<Control>().Expirations).IsEqualTo(1);
            await Assert.That(await database.Value("\"TimeoutCount\"", processId)).IsEqualTo(1);
            await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(2L);
            await Assert.That(await database.Count("relayer_outbox")).IsEqualTo(0L);
            // Even a new message identity cannot expire the same wait twice after inbox retention.
            await Deliver(restarted, scheduled with { MessageId = Guid.CreateVersion7().ToString("D") });
            await Assert.That(restarted.GetRequiredService<Control>().Expirations).IsEqualTo(1);
            await Assert.That(await database.Value("\"TimeoutCount\"", processId)).IsEqualTo(1);
        }
        finally { await worker.ReleaseAsync(default); }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_transition_rolls_back_timeout_state_and_inbox_then_redelivery_schedules_once(bool existing)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        var processId = Guid.NewGuid();
        if (existing) await Deliver(provider, new BeginWait(processId));
        var before = await database.WaitId(processId);
        var control = provider.GetRequiredService<Control>();
        control.AfterSave = static () => throw new InvalidOperationException("Rollback scheduled timeout.");
        var message = Encode(provider, new BeginWait(processId));
        await Assert.That(async () => await Deliver(provider, message)).Throws<InvalidOperationException>()
            .WithMessage("Rollback scheduled timeout.");
        await Assert.That(await database.WaitId(processId)).IsEqualTo(before);
        await Assert.That(await database.Count("relayer_outbox")).IsEqualTo(existing ? 1L : 0L);
        await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(existing ? 1L : 0L);
        control.AfterSave = null;
        await Deliver(provider, message);
        await Deliver(provider, message);
        await Assert.That(await database.WaitId(processId)).IsNotEqualTo(before);
        await Assert.That(await database.Count("relayer_outbox")).IsEqualTo(existing ? 2L : 1L);
        await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(existing ? 2L : 1L);
        await Assert.That(provider.GetRequiredService<Transport>().Messages.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Failed_timeout_processing_rolls_back_expiration_and_inbox_for_redelivery()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        var id = Guid.NewGuid();
        await Deliver(provider, new BeginWait(id));
        var wait = (await database.WaitId(id))!.Value;
        var timeout = Encode(provider, new WaitExpired(id, wait));
        var control = provider.GetRequiredService<Control>();
        control.AfterSave = static () => throw new InvalidOperationException("Rollback expiration.");
        await Assert.That(async () => await Deliver(provider, timeout)).Throws<InvalidOperationException>();
        await Assert.That(await database.WaitId(id)).IsEqualTo(wait);
        await Assert.That(await database.Value("\"TimeoutCount\"", id)).IsEqualTo(0);
        await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(1L);
        control.AfterSave = null;
        await Deliver(provider, timeout);
        await Deliver(provider, timeout);
        await Assert.That(await database.WaitId(id)).IsEqualTo(Guid.Empty);
        await Assert.That(await database.Value("\"TimeoutCount\"", id)).IsEqualTo(1);
        await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(2L);
    }

    [Test]
    public async Task Superseded_timeout_does_not_expire_new_wait_but_current_timeout_does()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        var id = Guid.NewGuid();
        await Deliver(provider, new BeginWait(id));
        var oldWait = (await database.WaitId(id))!.Value;
        await Deliver(provider, new BeginWait(id));
        var currentWait = (await database.WaitId(id))!.Value;
        await Deliver(provider, new WaitExpired(id, oldWait));
        await Assert.That(await database.WaitId(id)).IsEqualTo(currentWait);
        await Assert.That(await database.Value("\"TimeoutCount\"", id)).IsEqualTo(0);
        await Deliver(provider, new WaitExpired(id, currentWait));
        await Assert.That(await database.Value("\"TimeoutCount\"", id)).IsEqualTo(1);
        await Assert.That(provider.GetRequiredService<Control>().Expirations).IsEqualTo(1);
        await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(4L);
    }

    [Test]
    public async Task Completed_process_retains_tombstone_and_timeout_skips_transition()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        var id = Guid.NewGuid();
        await Deliver(provider, new BeginWait(id));
        var wait = (await database.WaitId(id))!.Value;
        await Deliver(provider, new Complete(id));
        var timeout = Encode(provider, new WaitExpired(id, wait));
        await Deliver(provider, timeout);
        await Deliver(provider, timeout);
        await Assert.That(provider.GetRequiredService<Control>().TimeoutTransitions).IsEqualTo(0);
        await Assert.That(await database.Value("\"TimeoutCount\"", id)).IsEqualTo(0);
        await Assert.That(await database.Count("processes")).IsEqualTo(1L);
        await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(3L);
    }

    [Test]
    public async Task Timeout_for_missing_process_fails_explicitly_and_does_not_create_state()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await Assert.That(async () => await Deliver(provider, new WaitExpired(Guid.NewGuid(), Guid.NewGuid())))
            .Throws<InvalidOperationException>().WithMessageContaining("No saga state exists");
        await Assert.That(await database.Count("processes")).IsEqualTo(0L);
        await Assert.That(await database.Count("relayer_inbox")).IsEqualTo(0L);
        await Assert.That(await database.Count("relayer_outbox")).IsEqualTo(0L);
    }

    private static ConsumeMessage Encode<T>(ServiceProvider provider, T message)
    {
        var body = new ArrayBufferWriter<byte>();
        provider.GetRequiredService<IMessageSerializer>().Serialize(message, body);
        provider.GetRequiredService<MessageTypeRegistry>().TryGetMessageType(typeof(T), out var identity);
        return new() { MessageType = identity,
            MessageId = Guid.CreateVersion7().ToString("D"), Body = new ReadOnlySequence<byte>(body.WrittenMemory) };
    }
    private static Task Deliver<T>(ServiceProvider provider, T message) => Deliver(provider, Encode(provider, message));
    private static async Task Deliver(ServiceProvider provider, ConsumeMessage message)
    {
        var handlers = provider.GetRequiredService<HandlerRegistry>();
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(),
            static (_, error, _) => ValueTask.FromException(error))
            .Handler(handlers.GetHandler<BeginWait, BeginProcess>())
            .Handler(handlers.GetHandler<WaitExpired, TimeoutProcess>())
            .Handler(handlers.GetHandler<Complete, CompleteProcess>())
            .Saga<Context, State>("timeout.process").Use(new AfterSave(provider.GetRequiredService<Control>())).Build();
        using var context = pipeline.RentContext(message, default);
        await pipeline.ProcessAsync(context);
    }

    [Message("timeout.begin")]
    public sealed record BeginWait(Guid ProcessId);
    [Message("timeout.expired")]
    public sealed record WaitExpired(Guid ProcessId, Guid WaitId);
    [Message("timeout.complete")]
    public sealed record Complete(Guid ProcessId);
    public sealed class State : EntityFrameworkCoreSagaData
    {
        public Guid WaitId { get; set; }
        public int TimeoutCount { get; set; }
    }
    public sealed class Control
    {
        public TimeProvider Clock { get; } = new Clock();
        public Func<ValueTask>? AfterSave;
        public int TimeoutLoads, TimeoutTransitions, Expirations;
    }
    private sealed class Clock : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);
    }
    public sealed class BeginProcess(Context database, ISender sender, Control control)
        : EntityFrameworkCoreSaga<Context, State, BeginWait>(database)
    {
        protected override ValueTask<State?> LoadAsync(BeginWait message, CancellationToken cancellationToken)
            => new(Database.Set<State>().SingleOrDefaultAsync(s => s.Id == message.ProcessId, cancellationToken));
        protected override State Create(BeginWait message) => new() { Id = message.ProcessId };
        protected override ValueTask TransitionAsync(State state, BeginWait message, MessageContext context, CancellationToken cancellationToken)
        {
            state.WaitId = Guid.NewGuid();
            var due = control.Clock.GetUtcNow().AddDays(1);
            return sender.SendAsync(new WaitExpired(state.Id, state.WaitId), o => o.DeliverAt = due, cancellationToken);
        }
    }
    public sealed class TimeoutProcess(Context database, Control control)
        : EntityFrameworkCoreSaga<Context, State, WaitExpired>(database)
    {
        protected override ValueTask<State?> LoadAsync(WaitExpired message, CancellationToken cancellationToken)
        {
            control.TimeoutLoads++;
            return new(Database.Set<State>().SingleOrDefaultAsync(s => s.Id == message.ProcessId, cancellationToken));
        }
        protected override ValueTask TransitionAsync(State state, WaitExpired message, MessageContext context, CancellationToken cancellationToken)
        {
            control.TimeoutTransitions++;
            if (state.WaitId == Guid.Empty || state.WaitId != message.WaitId) return default;
            state.WaitId = Guid.Empty;
            state.TimeoutCount++;
            control.Expirations++;
            return default;
        }
    }
    public sealed class CompleteProcess(Context database) : EntityFrameworkCoreSaga<Context, State, Complete>(database)
    {
        protected override ValueTask<State?> LoadAsync(Complete message, CancellationToken cancellationToken)
            => new(Database.Set<State>().SingleOrDefaultAsync(s => s.Id == message.ProcessId, cancellationToken));
        protected override ValueTask TransitionAsync(State state, Complete message, MessageContext context, CancellationToken cancellationToken)
        { state.IsCompleted = true; return default; }
    }
    public sealed class Context(DbContextOptions<Context> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var state = modelBuilder.Entity<State>().ToTable("processes");
            state.HasKey(s => s.Id);
            state.Property(s => s.Id).ValueGeneratedNever();
            state.Property(s => s.Version).IsRowVersion();
        }
    }
    private sealed class AfterSave(Control control) : IConsumeMiddleware
    {
        public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
        { await next(context); if (control.AfterSave is { } after) await after(); }
    }
    public sealed class Empty;
    private sealed class Transport : ITransport
    {
        public string Name => "events";
        internal List<ConsumeMessage> Messages { get; } = [];
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Record(message);
            return default;
        }
        private void Record(OutgoingMessage message)
        {
            if (message.DeliverAt is not null) throw new NotSupportedException("Native scheduling is unavailable.");
            var headers = message.Headers.ToArray();
            if (headers.Single(h => h.Key == "ce_type").Value.ToString() != "timeout.expired;v=1")
                throw new InvalidOperationException("Unexpected timeout wire type.");
            Messages.Add(new() { MessageType = new("timeout.expired", 1),
                MessageId = headers.Single(h => h.Key == "ce_id").Value.ToString(),
                Body = new ReadOnlySequence<byte>(message.Body.ToArray()) });
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        { foreach (var message in messages.Span) Record(message); return default; }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
    private sealed class Database : IAsyncDisposable
    {
        private readonly PostgreSqlFixture _fixture = new();
        internal NpgsqlDataSource Source { get; private set; } = null!;
        internal async Task Start()
        {
            await _fixture.StartAsync();
            Source = NpgsqlDataSource.Create(_fixture.ConnectionString);
            await Execute(PostgreSqlOutboxSchema.CreateScript() + "CREATE TABLE processes (\"Id\" uuid PRIMARY KEY, \"WaitId\" uuid NOT NULL, \"TimeoutCount\" integer NOT NULL, \"IsCompleted\" boolean NOT NULL)");
        }
        internal ServiceProvider Services()
        {
            var services = new ServiceCollection().AddLogging();
            services.AddSingleton<Control>();
            services.AddSingleton<Transport>();
            services.AddSingleton<ITransport>(sp => sp.GetRequiredService<Transport>());
            services.AddRelayerEntityFrameworkCoreOutbox();
            services.AddRelayerPostgreSqlMaintenance("timeouts", Source, new PostgreSqlMaintenanceOptions { RecoveryDelay = TimeSpan.Zero });
            services.AddDbContext<Context>((sp, o) => o.UseNpgsql(_fixture.ConnectionString).AddRelayerOutbox(sp));
            services.AddRelayer(r =>
            {
                r.Infrastructure().MessageTypes.Add<BeginWait>("timeout.begin");
                r.Infrastructure().MessageTypes.Add<WaitExpired>("timeout.expired");
                r.Infrastructure().MessageTypes.Add<Complete>("timeout.complete");
                r.UseJsonSerializer(RelayerGeneratedJsonContext.Default);
                new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<BeginProcess>(); new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<TimeoutProcess>(); new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<CompleteProcess>();
                r.AddTransport<Empty, Empty, Empty>("events", t => t.Send<WaitExpired>("timeouts", route =>
                    route.SendOptions = new() { RequiredDeliveryDelay = TimeSpan.FromDays(1), TransactionRequirement = SendTransactionRequirement.Outbox }));
            });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }
        internal async Task Execute(string sql)
        { await using var command = Source.CreateCommand(sql); await command.ExecuteNonQueryAsync(); }
        internal async Task<long> Count(string table)
        { await using var command = Source.CreateCommand($"SELECT count(*) FROM {table}"); return (long)(await command.ExecuteScalarAsync())!; }
        internal async Task<int> Value(string column, Guid id)
        {
            await using var command = Source.CreateCommand($"SELECT {column} FROM processes WHERE \"Id\" = @id");
            command.Parameters.AddWithValue("id", id);
            return (int)(await command.ExecuteScalarAsync())!;
        }
        internal async Task<Guid?> WaitId(Guid id)
        {
            await using var command = Source.CreateCommand("SELECT \"WaitId\" FROM processes WHERE \"Id\" = @id");
            command.Parameters.AddWithValue("id", id);
            return (Guid?)await command.ExecuteScalarAsync();
        }
        public async ValueTask DisposeAsync()
        { if (Source is not null) await Source.DisposeAsync(); await _fixture.DisposeAsync(); }
    }
}
