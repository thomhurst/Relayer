using System.Buffers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Sagas.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class SagaTests
{
    [Test]
    public async Task State_inbox_and_sends_commit_together_and_redelivery_skips_transition()
    {
        await using var database = new TestDatabase();
        await database.Start();
        await using var provider = database.Services();
        var control = provider.GetRequiredService<Control>();
        control.AfterSave = async _ => await AssertCounts(database, 0, 0, 0);
        var pipeline = Pipeline(provider).Build();
        var message = Message();
        await Deliver(pipeline, message);
        await Deliver(pipeline, message);
        await AssertCounts(database, 1, 1, 1);
        await Assert.That(control.Transitions).IsEqualTo(1);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    [Arguments("handler")]
    [Arguments("cancel")]
    [Arguments("retry")]
    [Arguments("deadletter")]
    [Arguments("outbox")]
    public async Task Failure_after_state_save_rolls_back_entire_transition(string failure)
    {
        await using var database = new TestDatabase();
        await database.Start();
        await using var provider = database.Services();
        var control = provider.GetRequiredService<Control>();
        using var cancellation = new CancellationTokenSource();
        control.AfterSave = context =>
        {
            switch (failure)
            {
                case "handler": throw new InvalidOperationException("Transition failed after save.");
                case "cancel": cancellation.Cancel(); break;
                case "retry": context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.Zero); break;
                case "deadletter": context.Features.GetRequired<ISettlementFeature>().DeadLetter("Rejected"); break;
            }
            return default;
        };
        if (failure == "outbox")
            await database.Execute("ALTER TABLE relayer_outbox ADD CONSTRAINT reject_rows CHECK (false)");
        var pipeline = Pipeline(provider).Build();
        var message = Message();
        if (failure is "retry" or "deadletter") await Deliver(pipeline, message);
        else if (failure == "cancel")
            await Assert.That(async () => await Deliver(pipeline, message, cancellation.Token)).Throws<OperationCanceledException>();
        else if (failure == "outbox")
            await Assert.That(async () => await Deliver(pipeline, message)).Throws<PostgresException>();
        else
            await Assert.That(async () => await Deliver(pipeline, message)).Throws<InvalidOperationException>();
        await AssertCounts(database, 0, 0, 0);
        if (failure == "outbox") await database.Execute("ALTER TABLE relayer_outbox DROP CONSTRAINT reject_rows");
        control.AfterSave = null;
        await Deliver(pipeline, message);
        await AssertCounts(database, 1, 1, 1);
        await Assert.That(control.Transitions).IsEqualTo(2);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Concurrent_creation_and_updates_retry_whole_transition_without_duplicate_sends(bool existing, bool sendOnly)
    {
        await using var database = new TestDatabase();
        await database.Start();
        if (existing) await database.Execute($"INSERT INTO processes (\"Id\", \"Count\", \"IsCompleted\") VALUES ('{TestDatabase.ProcessId}', 0, false)");
        await using var firstProvider = database.Services();
        await using var secondProvider = database.Services();
        var firstControl = firstProvider.GetRequiredService<Control>();
        var secondControl = secondProvider.GetRequiredService<Control>();
        firstControl.SendOnly = secondControl.SendOnly = sendOnly;
        var firstLoaded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondLoaded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCommitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        firstControl.AfterLoad = async () =>
        {
            firstLoaded.TrySetResult();
            await releaseFirst.Task.WaitAsync(TimeSpan.FromSeconds(20));
        };
        secondControl.AfterLoad = async () =>
        {
            if (secondControl.Loads != 1) return;
            secondLoaded.TrySetResult();
            await firstCommitted.Task.WaitAsync(TimeSpan.FromSeconds(20));
        };
        var first = Deliver(Pipeline(firstProvider, retries: true).Build(), Message());
        Task? second = null;
        try
        {
            await firstLoaded.Task.WaitAsync(TimeSpan.FromSeconds(20));
            second = Deliver(Pipeline(secondProvider, retries: true).Build(), Message());
            await secondLoaded.Task.WaitAsync(TimeSpan.FromSeconds(20));
            releaseFirst.TrySetResult();
            await first;
            firstCommitted.TrySetResult();
            await second;
        }
        finally
        {
            releaseFirst.TrySetResult();
            firstCommitted.TrySetResult();
            await Task.WhenAll(first, second ?? Task.CompletedTask);
        }
        await AssertCounts(database, sendOnly ? 0 : 2, 2, 2);
        await Assert.That(await database.Scalar("SELECT count(*) FROM processes")).IsEqualTo(1L);
        await Assert.That(secondControl.Loads).IsEqualTo(2);
        await Assert.That(secondControl.Contexts[0]).IsNotSameReferenceAs(secondControl.Contexts[1]);
        await Assert.That(secondControl.Errors.Count).IsEqualTo(1);
        if (existing)
            await Assert.That(secondControl.Errors[0]).IsTypeOf<DbUpdateConcurrencyException>();
        else
        {
            var error = secondControl.Errors[0] as DbUpdateException;
            await Assert.That(error?.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation }).IsTrue();
        }
        await Assert.That(firstProvider.GetRequiredService<Transport>().Sends + secondProvider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    public async Task Completed_process_is_retained_and_later_messages_do_not_transition()
    {
        await using var database = new TestDatabase();
        await database.Start();
        await using var provider = database.Services();
        var control = provider.GetRequiredService<Control>();
        control.Complete = true;
        var pipeline = Pipeline(provider).Build();
        await Deliver(pipeline, Message());
        await Deliver(pipeline, Message());
        await AssertCounts(database, 1, 2, 1);
        await Assert.That(control.Transitions).IsEqualTo(1);
        await Assert.That(await database.Scalar("SELECT count(*) FROM processes WHERE \"IsCompleted\"")).IsEqualTo(1L);
    }

    [Test]
    public async Task Missing_nonstarting_state_and_untracked_queries_fail_explicitly()
    {
        await using var database = new TestDatabase();
        await database.Start();
        await using var provider = database.Services();
        var control = provider.GetRequiredService<Control>();
        control.CanStart = false;
        await Assert.That(async () => await Deliver(Pipeline(provider).Build(), Message())).Throws<InvalidOperationException>()
            .WithMessage("No saga state exists for this message. Override Create only for messages that may start the process.");
        await AssertCounts(database, 0, 0, 0);
        await database.Execute($"INSERT INTO processes (\"Id\", \"Count\", \"IsCompleted\") VALUES ('{TestDatabase.ProcessId}', 0, false)");
        control.NoTracking = true;
        await Assert.That(async () => await Deliver(Pipeline(provider).Build(), Message())).Throws<InvalidOperationException>()
            .WithMessage("LoadAsync must return tracked saga state from the selected scoped DbContext. Do not use AsNoTracking.");
        await AssertCounts(database, 0, 0, 0);
    }

    [Test]
    [Arguments(false, "valid")]
    [Arguments(true, "key")]
    [Arguments(true, "version")]
    public async Task Invalid_native_configuration_fails_at_composition(bool outbox, string model)
    {
        await using var database = new TestDatabase();
        await using var provider = database.Services(outbox, model);
        await Assert.That(() => Pipeline(provider)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Missing_transaction_boundary_fails_before_loading_state()
    {
        await using var database = new TestDatabase();
        await using var provider = database.Services();
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Process>()).Build();
        await Assert.That(async () => await Deliver(pipeline, Message())).Throws<InvalidOperationException>()
            .WithMessage("Saga handling requires the Saga consumer transaction boundary for the same scoped DbContext.");
    }

    private static ConsumePipelineBuilder Pipeline(ServiceProvider provider, bool retries = false)
    {
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        var messages = provider.GetRequiredService<MessageTypeRegistry>();
        var control = provider.GetRequiredService<Control>();
        var builder = retries
            ? new ConsumePipelineBuilder(scopes, messages, new Serializer(),
                new FailurePipeline(provider, "events", "source", "process", new() { RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 }),
                new Classifier(control))
            : new ConsumePipelineBuilder(scopes, messages, new Serializer(), static (_, error, _) => ValueTask.FromException(error));
        var declaration = new ConsumerBuilder<object>(new ServiceCollection(), new HandlerRegistry(), "source");
        declaration.Saga<Context, State>("process");
        declaration.ConfigurePipeline(pipeline => pipeline.Use(new AfterSave(control)));
        return builder.Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Process>())
            .ConfigureConsumer(provider, declaration.Build());
    }

    private static ConsumeMessage Message() => new()
    {
        MessageType = new("saga.probe", 1), MessageId = Guid.CreateVersion7().ToString("D"),
        Body = new ReadOnlySequence<byte>(new byte[] { 1 })
    };

    private static async Task Deliver(ConsumePipeline pipeline, ConsumeMessage message, CancellationToken cancellation = default)
    {
        using var context = pipeline.RentContext(message, cancellation);
        await pipeline.ProcessAsync(context);
    }

    private static async Task AssertCounts(TestDatabase database, long state, long inbox, long outbox)
    {
        await Assert.That(await database.Scalar("SELECT coalesce(sum(\"Count\"), 0)::bigint FROM processes")).IsEqualTo(state);
        await Assert.That(await database.Scalar("SELECT count(*) FROM relayer_inbox")).IsEqualTo(inbox);
        await Assert.That(await database.Scalar("SELECT count(*) FROM relayer_outbox")).IsEqualTo(outbox);
    }

    public sealed class State : EntityFrameworkCoreSagaData { public int Count { get; set; } }
    [Message("saga.probe")]
    public sealed record Probe;
    public sealed class Empty;

    public sealed class Control
    {
        public int Loads, Transitions;
        public bool Complete, SendOnly, NoTracking;
        public bool CanStart = true;
        public string Model = "valid";
        public Func<ValueTask>? AfterLoad;
        public Func<MessageContext, ValueTask>? AfterSave;
        public List<DbContext> Contexts { get; } = [];
        public List<Exception> Errors { get; } = [];
    }

    public sealed class Process(Context database, ISender sender, Control control) : EntityFrameworkCoreSaga<Context, State, Probe>(database)
    {
        protected override async ValueTask<State?> LoadAsync(Probe message, CancellationToken cancellationToken)
        {
            control.Loads++;
            control.Contexts.Add(Database);
            var query = Database.Set<State>().AsQueryable();
            if (control.NoTracking) query = query.AsNoTracking();
            var state = await query.SingleOrDefaultAsync(s => s.Id == TestDatabase.ProcessId, cancellationToken);
            if (control.AfterLoad is { } after) await after();
            return state;
        }
        protected override State Create(Probe message) => control.CanStart ? new() { Id = TestDatabase.ProcessId } : base.Create(message);
        protected override async ValueTask TransitionAsync(State state, Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            control.Transitions++;
            if (!control.SendOnly) state.Count++;
            state.IsCompleted = control.Complete;
            await sender.SendAsync(message, cancellationToken);
        }
    }

    public sealed class Context(DbContextOptions<Context> options, Control control) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var state = modelBuilder.Entity<State>().ToTable("processes");
            state.HasKey(s => s.Id);
            if (control.Model != "key") state.Property(s => s.Id).ValueGeneratedNever();
            if (control.Model != "version") state.Property(s => s.Version).IsRowVersion();
        }
    }
    private sealed class AfterSave(Control control) : IConsumeMiddleware
    {
        public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
        {
            await next(context);
            if (control.AfterSave is { } after) await after(context);
        }
    }
    private sealed class Classifier(Control control) : IFailureClassifier
    {
        public FailureAction Classify(Exception exception, MessageContext context)
        {
            control.Errors.Add(exception);
            return FailureAction.Retry;
        }
    }
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        { destination.GetSpan(1)[0] = 1; destination.Advance(1); }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)new Probe();
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
    private sealed class TestDatabase : IAsyncDisposable
    {
        internal static Guid ProcessId => new("018f0000-0000-7000-8000-000000000001");
        private readonly PostgreSqlFixture _fixture = new();
        internal async Task Start()
        {
            await _fixture.StartAsync();
            await Execute(PostgreSqlOutboxSchema.CreateScript() +
                "CREATE TABLE processes (\"Id\" uuid PRIMARY KEY, \"Count\" integer NOT NULL, \"IsCompleted\" boolean NOT NULL)");
        }
        internal ServiceProvider Services(bool outbox = true, string model = "valid")
        {
            var services = new ServiceCollection();
            services.AddSingleton(new Control { Model = model });
            services.AddSingleton<Transport>();
            services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<Transport>());
            services.AddSingleton<IMessageSerializer, Serializer>();
            services.AddRelayerEntityFrameworkCoreOutbox();
            services.AddDbContext<Context>((sp, o) =>
            {
                o.UseNpgsql(string.IsNullOrEmpty(_fixture.ConnectionString) ? "Host=unused" : _fixture.ConnectionString);
                // Each invalid model has a distinct service provider/model cache.
                o.EnableServiceProviderCaching(false);
                if (outbox) o.AddRelayerOutbox(sp);
            });
            services.AddRelayer(r =>
            {
                r.MessageTypes.Add<Probe>("saga.probe");
                r.Handler<Process>();
                r.AddTransport<Empty, Empty, Empty>("events", t => t.Send<Probe>("destination"));
            });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }
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
        public ValueTask DisposeAsync() => _fixture.DisposeAsync();
    }
}
