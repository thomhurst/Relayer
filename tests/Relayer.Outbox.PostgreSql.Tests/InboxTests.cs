using System.Buffers;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.TestFixtures;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

[InheritsTests]
public class InboxTests : InboxContractTests
{
    [Test]
    public async Task Orders_model_is_ready_before_concurrent_database_work()
    {
        var options = new DbContextOptionsBuilder<OrdersContext>().UseNpgsql("Host=unused").Options;
        await using var context = new OrdersContext(options, new Control());
        // Reading Model constructs the native runtime model without issuing a database operation.
        // EF's first-use race is reachable if the relational model is still lazy at this point.
        await Assert.That(context.Model.FindRuntimeAnnotationValue(RelationalAnnotationNames.RelationalModel)).IsNotNull();
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
    public async Task Explicit_inbox_scopes_identity_by_consumer_without_same_context_outbox()
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
    [Arguments(null)]
    [Arguments(IsolationLevel.Serializable)]
    public async Task Outbox_write_failure_rolls_back_and_redelivery_runs_handler(IsolationLevel? isolationLevel)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true);
        await database.Execute("ALTER TABLE \"Case\"\"Inbox\".\"test_outbox\" ADD CONSTRAINT reject_rows CHECK (false)");
        var settled = 0;
        var pipeline = Pipeline(provider, isolationLevel: isolationLevel).Settle(_ => { settled++; return default; }).Build();
        var message = Message();
        await Assert.That(async () => await Deliver(pipeline, message)).Throws<PostgresException>();
        await Assert.That(settled).IsEqualTo(0);
        await Assert.That(await database.Count("inbox")).IsEqualTo(0L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(0L);
        await Assert.That(await database.Orders()).IsEqualTo(0L);
        await database.Execute("ALTER TABLE \"Case\"\"Inbox\".\"test_outbox\" DROP CONSTRAINT reject_rows");
        await Deliver(pipeline, message);
        await Assert.That(provider.GetRequiredService<Control>().Invocations).IsEqualTo(2);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
    }

    [Test]
    [Arguments(false, false, false, "retry")]
    [Arguments(false, false, false, "deadletter")]
    [Arguments(false, false, true, "retry")]
    [Arguments(false, false, true, "deadletter")]
    [Arguments(false, true, false, "retry")]
    [Arguments(false, true, false, "deadletter")]
    [Arguments(false, true, true, "retry")]
    [Arguments(false, true, true, "deadletter")]
    [Arguments(true, false, false, "retry")]
    [Arguments(true, false, false, "deadletter")]
    [Arguments(true, false, true, "retry")]
    [Arguments(true, false, true, "deadletter")]
    [Arguments(true, true, false, "retry")]
    [Arguments(true, true, false, "deadletter")]
    [Arguments(true, true, true, "retry")]
    [Arguments(true, true, true, "deadletter")]
    [Arguments(true, false, false, "transient")]
    [Arguments(true, true, false, "transient")]
    public async Task Settlement_requests_during_disposal_cannot_change_a_committed_outcome(bool nativeRetries, bool failures, bool duplicate, string request)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true, retries: nativeRetries);
        var control = provider.GetRequiredService<Control>();
        var failurePipeline = failures ? new FailurePipeline(provider, "events", "source", "orders", new()) : null;
        var settled = 0;
        var pipeline = Pipeline(provider, failures: failurePipeline).Settle(_ => { settled++; return default; }).Build();
        var message = Message();
        if (duplicate) await Deliver(pipeline, message);
        using var context = pipeline.RentContext(message);
        control.OnDispose = () =>
        {
            if (request == "transient") throw new NpgsqlException("Transient cleanup failure", new TimeoutException());
            var settlement = context.Features.GetRequired<ISettlementFeature>();
            if (request == "retry") settlement.Retry(TimeSpan.Zero);
            else settlement.DeadLetter("Rejected during disposal");
        };
        if (request == "transient")
            await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<NpgsqlException>()
                .WithMessage("Transient cleanup failure");
        else
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
    [Arguments(false)]
    [Arguments(true)]
    public async Task Immediate_retry_uses_a_fresh_scope_and_commits_only_the_successful_attempt(bool nativeRetries)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true, retries: nativeRetries);
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
    [Arguments(false)]
    [Arguments(true)]
    public async Task Serializable_conflicting_deliveries_replay_the_complete_handler_in_a_fresh_scope(bool nativeRetries)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true, retries: nativeRetries);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var bothRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCommitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstMessage = Message();
        var secondMessage = Message();
        var attempts = new ConcurrentBag<(string Message, IServiceProvider Scope, OrdersContext Context)>();
        var reads = 0;
        var control = provider.GetRequiredService<Control>();
        control.BeforeSave = async context =>
        {
            var db = context.Services.GetRequiredService<OrdersContext>();
            attempts.Add((context.MessageId, context.Services, db));
            await Assert.That(db.Database.CurrentTransaction!.GetDbTransaction().IsolationLevel).IsEqualTo(IsolationLevel.Serializable);
            await db.Set<Order>().CountAsync(timeout.Token);
            if (Interlocked.Increment(ref reads) == 2) bothRead.TrySetResult();
            await bothRead.Task.WaitAsync(timeout.Token);
            // Both snapshots read the same predicate. Finish the winner before the loser writes,
            // so its replay starts after that commit rather than racing an in-flight commit again.
            if (context.MessageId == secondMessage.MessageId)
                await firstCommitted.Task.WaitAsync(timeout.Token);
        };
        var failures = new FailurePipeline(provider, "events", "source", "orders", new());
        var completions = 0;
        var pipeline = Pipeline(provider, failures: failures, isolationLevel: IsolationLevel.Serializable)
            .Settle(context =>
            {
                if (context.Features.GetRequired<ISettlementFeature>().RequestedDisposition is { Kind: DispositionKind.Complete })
                    Interlocked.Increment(ref completions);
                return default;
            }).Build();
        var first = Deliver(pipeline, firstMessage, timeout.Token);
        var second = Deliver(pipeline, secondMessage, timeout.Token);
        try { await first; }
        finally { firstCommitted.TrySetResult(); }
        await second;
        await Assert.That(control.Invocations).IsEqualTo(3);
        await Assert.That(completions).IsEqualTo(2);
        var replay = attempts.GroupBy(x => x.Message).Single(x => x.Count() == 2).ToArray();
        await Assert.That(replay[0].Scope).IsNotSameReferenceAs(replay[1].Scope);
        await Assert.That(replay[0].Context).IsNotSameReferenceAs(replay[1].Context);
        await Assert.That(await database.Orders()).IsEqualTo(2L);
        await Assert.That(await database.Count("inbox")).IsEqualTo(2L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(2L);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(0);
    }

    [Test]
    [Arguments(IsolationLevel.RepeatableRead, false)]
    [Arguments(IsolationLevel.RepeatableRead, true)]
    [Arguments(IsolationLevel.Serializable, false)]
    [Arguments(IsolationLevel.Serializable, true)]
    public async Task Snapshot_duplicate_racing_original_retries_the_inbox_without_repeating_handler(IsolationLevel isolationLevel, bool nativeRetries)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true, retries: nativeRetries);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var saved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var control = provider.GetRequiredService<Control>();
        control.AfterSave = async _ =>
        {
            saved.TrySetResult();
            await release.Task.WaitAsync(timeout.Token);
        };
        var failures = new FailurePipeline(provider, "events", "source", "orders", new());
        var completions = 0;
        var pipeline = Pipeline(provider, failures: failures, isolationLevel: isolationLevel)
            .Settle(context =>
            {
                if (((DefaultMessageContext)context).ObservedDisposition.Kind == DispositionKind.Complete)
                    Interlocked.Increment(ref completions);
                return default;
            }).Build();
        var message = Message();
        var original = Deliver(pipeline, message, timeout.Token);
        await saved.Task.WaitAsync(timeout.Token);
        var duplicate = Deliver(pipeline, message, timeout.Token);
        try { await database.WaitForInboxLock(); }
        finally { release.TrySetResult(); }
        await Task.WhenAll(original, duplicate);
        await Assert.That(control.Invocations).IsEqualTo(1);
        await Assert.That(completions).IsEqualTo(2);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
    }

    [Test]
    [Arguments(PostgresErrorCodes.SerializationFailure, false, false)]
    [Arguments(PostgresErrorCodes.DeadlockDetected, true, false)]
    [Arguments(PostgresErrorCodes.SerializationFailure, false, true)]
    [Arguments(PostgresErrorCodes.DeadlockDetected, true, true)]
    public async Task Conflict_retry_exhaustion_uses_normal_failure_handling(string sqlState, bool wrapped, bool defaultRetries)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true);
        var control = provider.GetRequiredService<Control>();
        var scopes = new HashSet<IServiceProvider>();
        control.AfterSave = async context =>
        {
            scopes.Add(context.Services);
            try
            {
                var sql = sqlState == PostgresErrorCodes.SerializationFailure
                    ? "DO $conflict$ BEGIN RAISE EXCEPTION 'transaction conflict' USING ERRCODE = '40001'; END $conflict$"
                    : "DO $conflict$ BEGIN RAISE EXCEPTION 'transaction conflict' USING ERRCODE = '40P01'; END $conflict$";
                await context.Services.GetRequiredService<OrdersContext>().Database.ExecuteSqlRawAsync(sql);
            }
            catch (PostgresException error) when (wrapped) { throw new DbUpdateException("Save failed", error); }
        };
        var failures = new FailurePipeline(provider, "events", "source", "orders",
            defaultRetries ? new() : FailureOptions.None);
        var pipeline = Pipeline(provider, failures: failures, isolationLevel: IsolationLevel.Serializable).Build();
        using var context = pipeline.RentContext(Message());
        await pipeline.ProcessAsync(context);
        // Each handler-level attempt exhausts the four transaction-conflict attempts first.
        var expectedInvocations = defaultRetries ? 16 : 4;
        await Assert.That(control.Invocations).IsEqualTo(expectedInvocations);
        await Assert.That(scopes.Count).IsEqualTo(expectedInvocations);
        await Assert.That(context.ObservedDisposition.Kind).IsEqualTo(DispositionKind.DeadLetter);
        await Assert.That(await database.Orders()).IsEqualTo(0L);
        await Assert.That(await database.Count("inbox")).IsEqualTo(0L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(0L);
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
    public async Task Native_strategy_replays_a_transient_connection_failure_in_a_fresh_scope()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services(outbox: true, retries: true);
        var control = provider.GetRequiredService<Control>();
        var scopes = new List<IServiceProvider>();
        var contexts = new List<OrdersContext>();
        control.AfterSave = async context =>
        {
            scopes.Add(context.Services);
            await context.Services.GetRequiredService<ISender>().SendNowAsync(new Probe(2));
            var db = context.Services.GetRequiredService<OrdersContext>();
            contexts.Add(db);
            if (scopes.Count != 1) return;
            var connection = (NpgsqlConnection)db.Database.GetDbConnection();
            await database.Execute($"SELECT pg_terminate_backend({connection.ProcessID})");
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            await command.ExecuteScalarAsync();
        };
        var failures = new FailurePipeline(provider, "events", "source", "orders", new());
        await Deliver(Pipeline(provider, failures: failures).Build(), Message());
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(1);
        await Assert.That(control.Invocations).IsEqualTo(2);
        await Assert.That(scopes[0]).IsNotSameReferenceAs(scopes[1]);
        await Assert.That(contexts[0]).IsNotSameReferenceAs(contexts[1]);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
    }

    [Test]
    public async Task Native_strategy_verifies_an_unknown_commit_without_replaying_the_transaction()
    {
        await using var database = new Database();
        await database.Start();
        var fault = new LostCommitResponse();
        await using var provider = database.Services(outbox: true, retries: true, interceptor: fault);
        var settled = 0;
        await Deliver(Pipeline(provider).Settle(_ => { settled++; return default; }).Build(), Message());
        await Assert.That(fault.Transactions).IsEqualTo(1);
        await Assert.That(fault.Commits).IsEqualTo(1);
        await Assert.That(provider.GetRequiredService<Control>().Invocations).IsEqualTo(1);
        await Assert.That(settled).IsEqualTo(1);
        await Assert.That(await database.Orders()).IsEqualTo(1L);
        await Assert.That(await database.Count("inbox")).IsEqualTo(1L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(1L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Unknown_commit_verification_preserves_buffered_sends(bool outbox)
    {
        await using var database = new Database();
        await database.Start();
        var fault = new LostCommitResponse();
        await using var provider = database.Services(outbox, retries: true, interceptor: fault);
        var control = provider.GetRequiredService<Control>();
        control.AfterSave = async context =>
        {
            var sender = context.Services.GetRequiredService<ISender>();
            if (outbox) await sender.SendNowAsync(new Probe(2));
            else await sender.SendAsync(new Probe(2));
        };
        var failures = new FailurePipeline(provider, "events", "source", "orders", new());
        var settled = 0;
        await Deliver(Pipeline(provider, inbox: !outbox, failures: failures)
            .Settle(_ => { settled++; return default; }).Build(), Message());
        await Assert.That(fault.Commits).IsEqualTo(1);
        await Assert.That(control.Invocations).IsEqualTo(1);
        await Assert.That(provider.GetRequiredService<Transport>().Sends).IsEqualTo(1);
        await Assert.That(settled).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Verification_scope_cleanup_errors_propagate_without_replaying_work(bool committed)
    {
        await using var database = new Database();
        await database.Start();
        var fault = new LostCommitResponse();
        await using var provider = database.Services(outbox: true, retries: true, interceptor: committed ? fault : null);
        var control = provider.GetRequiredService<Control>();
        if (!committed) control.AfterSave = _ => throw new NpgsqlException("Unknown transaction outcome", new IOException());
        var disposals = 0;
        control.OnDispose = () =>
        {
            if (++disposals == 2) throw new NpgsqlException("Verification cleanup failure", new TimeoutException());
        };
        var failures = new FailurePipeline(provider, "events", "source", "orders", new());
        var settled = 0;
        var pipeline = Pipeline(provider, failures: failures).Settle(_ => { settled++; return default; }).Build();
        var message = Message();
        await Assert.That(async () => await Deliver(pipeline, message)).Throws<NpgsqlException>()
            .WithMessage("Verification cleanup failure");
        control.OnDispose = null;
        control.AfterSave = null;
        await Assert.That(settled).IsEqualTo(0);
        await Assert.That(fault.Transactions).IsEqualTo(committed ? 1 : 0);
        await Assert.That(control.Invocations).IsEqualTo(1);
        await Assert.That(await database.Count("inbox")).IsEqualTo(committed ? 1L : 0L);
        await Assert.That(await database.Count("outbox")).IsEqualTo(committed ? 1L : 0L);
        await Assert.That(await database.Orders()).IsEqualTo(committed ? 1L : 0L);
        await Deliver(pipeline, message);
        await Assert.That(control.Invocations).IsEqualTo(committed ? 1 : 2);
        await Assert.That(settled).IsEqualTo(1);
    }

    private sealed class LostCommitResponse : DbTransactionInterceptor
    {
        internal int Transactions;
        internal int Commits;
        public override ValueTask<DbTransaction> TransactionStartedAsync(DbConnection connection, TransactionEndEventData eventData,
            DbTransaction result, CancellationToken cancellationToken = default)
        {
            Transactions++;
            return new(result);
        }
        public override async Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData,
            CancellationToken cancellationToken = default)
        {
            if (++Commits != 1) return;
            // The database has committed, but the client cannot observe a successful response.
            await eventData.Context!.Database.GetDbConnection().CloseAsync();
            throw new NpgsqlException("Injected lost commit response", new IOException("Connection lost after commit"));
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Inbox_follows_the_outbox_context_configuration_of_each_delivery_scope(bool nativeRetries)
    {
        await using var database = new Database();
        await database.Start();
        var options = Database.Options;
        await using var provider = database.Services(outbox: true, retries: nativeRetries, contextOptions: () => options,
            interceptor: nativeRetries ? new LostCommitResponse() : null);
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

    protected override async Task<InboxFixture> CreateFixtureAsync()
    {
        var database = new Database();
        try
        {
            await database.Start();
            return new ContractFixture(database);
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }
    }

    private sealed class ContractFixture(Database database) : InboxFixture
    {
        public override InboxConsumer Consumer(string name = "orders") => new ContractConsumer(database.Services(outbox: true), name);
        public override async Task<DurableState> ReadStateAsync() =>
            new(await database.Orders(), await database.Count("inbox"), await database.Count("outbox"));
        public override Task WaitForContentionAsync() => database.WaitForInboxLock();
        public override ValueTask DisposeAsync() => database.DisposeAsync();
    }

    private sealed class ContractConsumer : InboxConsumer
    {
        private readonly ServiceProvider _provider;
        private readonly Control _control;
        private readonly ConsumePipeline _pipeline;

        public ContractConsumer(ServiceProvider provider, string name)
        {
            _provider = provider;
            _control = provider.GetRequiredService<Control>();
            _control.AfterSave = async context =>
            {
                await Assert.That(context.Services.GetRequiredService<OrdersContext>().Database.CurrentTransaction).IsNotNull();
                if (AfterSave is { } after) await after(context);
            };
            _pipeline = Pipeline(provider, name).Settle(_ => Settlement?.Invoke() ?? default).Build();
        }

        public override RelayerInstrumentation Instrumentation => _provider.GetRequiredService<RelayerInstrumentation>();
        public override int Invocations => _control.Invocations;
        public override int DirectSends => _provider.GetRequiredService<Transport>().Sends;
        public override Task DeliverAsync(Guid messageId, CancellationToken cancellation = default) =>
            Deliver(_pipeline, Message() with { MessageId = messageId.ToString("D") }, cancellation);
        public override ValueTask DisposeAsync() => _provider.DisposeAsync();
    }

    private static ConsumePipelineBuilder Pipeline(ServiceProvider provider, string consumer = "orders", bool inbox = false,
        FailurePipeline? failures = null, IsolationLevel? isolationLevel = null)
    {
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        var messages = provider.GetRequiredService<MessageTypeRegistry>();
        var builder = failures is null
            ? new ConsumePipelineBuilder(scopes, messages, new Serializer(), static (_, error, _) => ValueTask.FromException(error))
            : new ConsumePipelineBuilder(scopes, messages, new Serializer(), failures);
        var declaration = new ConsumerBuilder<Empty>(new ServiceCollection(), new HandlerRegistry(), "source");
        declaration.EntityFrameworkCore<OrdersContext>(consumer, inbox, inbox ? Database.Options : null, isolationLevel: isolationLevel);
        return builder.Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
            .ConfigureConsumer(provider, declaration.Build());
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
        public Func<MessageContext, ValueTask>? BeforeSave;
        public Func<MessageContext, ValueTask>? AfterSave;
        public Action? OnDispose;
    }

    public sealed class Handler(OrdersContext db, ISender sender, Control control) : IHandler<Probe>
    {
        public async ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref control.Invocations);
            if (control.BeforeSave is { } before) await before(context);
            db.Add(new Order { Id = Guid.CreateVersion7() });
            if (control.Outbox) await sender.SendAsync(new Probe(call), cancellationToken);
            await db.SaveChangesAsync(cancellationToken);
            if (control.AfterSave is { } after) await after(context);
        }
    }

    public sealed class OrdersContext(DbContextOptions<OrdersContext> options, Control control) : DbContext(options)
    {
        protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder) =>
            configurationBuilder.Conventions.Add(static _ => new InitializeRelationalModel());

        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<Order>().ToTable("orders").HasKey(x => x.Id);
        public override async ValueTask DisposeAsync()
        {
            await base.DisposeAsync();
            control.OnDispose?.Invoke();
        }
    }
    // EF 10.0.15 has a first-use relational-model race (dotnet/efcore#39214).
    // Build it in EF's existing model-construction pipeline before publishing the cached
    // runtime model. Tests still execute concurrently; remove after upgrading to 10.0.16.
    private sealed class InitializeRelationalModel : IModelFinalizedConvention
    {
        public IModel ProcessModelFinalized(IModel model)
        {
            _ = model.GetRelationalModel();
            return model;
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
        internal ServiceProvider Services(bool outbox, bool retries = false, Func<EntityFrameworkCoreOutboxOptions>? contextOptions = null, IInterceptor? interceptor = null)
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
                    native => { if (retries) native.EnableRetryOnFailure(3, TimeSpan.Zero, null); });
                if (interceptor is not null) o.AddInterceptors(interceptor);
                if (outbox) o.AddRelayerOutbox(sp, contextOptions?.Invoke() ?? Options);
            });
            // Outbox on another context must not turn on the inbox for OrdersContext.
            services.AddDbContext<OtherContext>((sp, o) => o.UseNpgsql("Host=unused").AddRelayerOutbox(sp));
            services.AddRelayer(r =>
            {
                r.MessageTypes.Add<Probe>("inbox.probe");
                new ConsumerBuilder<object>(r.Services, r.Handlers, "manual").Handler<Handler>();
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
                await using var command = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO %test_inbox%' AND datname = current_database()", connection);
                if ((long)(await command.ExecuteScalarAsync(timeout.Token))! > 0) return;
                await Task.Delay(10, timeout.Token);
            }
        }
        public ValueTask DisposeAsync() => _fixture.DisposeAsync();
    }
}
