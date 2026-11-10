using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.InMemory;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class OutboxTelemetryTests
{
    [Test]
    [Arguments("direct", 1)]
    [Arguments("direct", 2)]
    [Arguments("recovery", 2)]
    [Arguments("ordered", 2)]
    public async Task Request_creation_survives_durable_dispatch_and_consumer_processing(string mode, int count)
    {
        await using var database = new PostgreSqlFixture();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddRelayerEntityFrameworkCoreOutbox();
        services.AddRelayerPostgreSqlDirectDispatch("store", source);
        services.AddDbContext<Context>((provider, builder) => builder.UseNpgsql(database.ConnectionString)
            .AddRelayerOutbox(provider, new EntityFrameworkCoreOutboxOptions
            {
                Lanes = 1, DispatchStoreName = mode == "direct" ? "store" : null,
            }));
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Probe>("outbox.trace");
            builder.Handler<Handler>();
            builder.InMemory("local", transport =>
            {
                transport.Send<Probe>("orders");
                transport.Options.Configure(options => options.Endpoints["orders"] = new()
                {
                    Capabilities = new() { OrderedByKey = true, MaxBatchSize = 10 },
                });
            });
        });
        await using var provider = services.BuildServiceProvider();
        var tracing = provider.GetRequiredService<MessageTracing>();
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        using var capture = new Capture(tracing, instrumentation);
        ActivitySpanId request;
        ActivityTraceId requestTrace;
        using (var activity = new Activity("HTTP request").Start())
        {
            request = activity.SpanId;
            requestTrace = activity.TraceId;
            await using var scope = provider.CreateAsyncScope();
            var context = scope.ServiceProvider.GetRequiredService<Context>();
            using var selection = context.EnlistRelayerOutbox();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            for (var i = 0; i < count; i++)
                await sender.SendAsync(new Probe(i), options =>
                {
                    if (mode == "ordered") options.OrderingKey = "same-key";
                });
            await Assert.That(capture.Pending.Sum()).IsEqualTo(0);
            await Assert.That(capture.Sent.Sum()).IsEqualTo(0);
            await context.SaveChangesAsync();
        }
        var creates = capture.Activities.Where(a => a.OperationName == "create orders").ToArray();
        await Assert.That(creates.Length).IsEqualTo(count);
        await Assert.That(creates.All(a => a.Kind == ActivityKind.Producer && a.ParentSpanId == request &&
            a.GetTagItem("messaging.operation.type") as string == "create")).IsTrue();
        await Assert.That(capture.Pending.Sum()).IsEqualTo(count);
        await Assert.That(capture.Sent.Sum()).IsEqualTo(0);
        var store = new PostgreSqlOutboxStore(source);
        var rows = new OutboxRecord[count];
        var read = await store.ReadPendingAsync(new(mode == "ordered" ? 0 : null, DateTimeOffset.UtcNow), rows, default);
        await Assert.That(read).IsEqualTo(count);
        var persistedHeaders = rows.Select(row => Snapshot(row.Message) + (mode == "ordered"
            ? "|" + OutboxHeaders.Sequence + "=" + row.InsertSequence!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)
            : "")).ToArray();
        var persistedContexts = rows.Select(row => Read(row.Message)).ToArray();
        await Assert.That(persistedContexts.Select(c => c.SpanId)).IsEquivalentTo(creates.Select(a => a.SpanId));
        // A different dispatch activity must never replace a creation context in durable headers.
        using (var dispatch = new Activity("dispatch worker").Start())
        {
            if (mode == "ordered")
            {
                using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1 },
                    provider.GetServices<ITransport>(), NullLogger<PostgreSqlOrderedDispatcher>.Instance,
                    instrumentation: instrumentation);
                await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(count);
            }
            else
            {
                var worker = provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>("store");
                if (mode == "recovery") await worker.DispatchRecoveredAsync(rows, default);
                else
                {
                    // Start outside the request's ExecutionContext. The queue transfers payloads alone.
                    await worker.StartAsync(default);
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    await worker.StopAsync(timeout.Token);
                }
            }
        }
        var sends = capture.Activities.Where(a => a.OperationName == "send orders").ToArray();
        await Assert.That(sends.Length).IsEqualTo(mode == "ordered" ? count : 1);
        await Assert.That(sends.All(a => a.Kind == ActivityKind.Client &&
            a.GetTagItem("messaging.operation.type") as string == "send")).IsTrue();
        await Assert.That(sends.SelectMany(a => a.Links).Select(l => l.Context.SpanId))
            .IsEquivalentTo(creates.Select(a => a.SpanId));
        await Assert.That(capture.Sent.Sum()).IsEqualTo(count);
        await Assert.That(capture.Pending.Sum()).IsEqualTo(0);
        await Assert.That(capture.Lags.Count).IsEqualTo(count);
        await Assert.That(capture.Lags.All(lag => lag >= 0)).IsTrue();
        await Assert.That(await store.GetPendingCountAsync(DateTimeOffset.UtcNow, default)).IsEqualTo(0L);

        // Consume only after dispatch completes, without any ambient activity.
        var transport = (InMemoryTransport)provider.GetServices<ITransport>().Single();
        var receivedHeaders = new ConcurrentQueue<string>();
        var receivedSequences = new ConcurrentQueue<long?>();
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), provider.GetRequiredService<IMessageSerializer>(),
            static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
            .Tracing(tracing, new() { System = InMemoryTransport.MessagingSystem, Destination = "orders" })
            .Settle(context =>
            {
                receivedSequences.Enqueue(context.OutboxSequence);
                var native = context.Features.GetRequired<NativeMessage<InMemoryDelivery>>();
                native.Message.Complete();
                native.Disposition = Disposition.SettledByHandler;
                return default;
            }).Build(1);
        var receive = new ReceivePipeline<InMemoryDelivery, InMemoryMessageBinding>(pipeline, new HeaderNames(), 1);
        transport.RegisterConsumer("orders", (delivery, token) =>
        {
            receivedHeaders.Enqueue(string.Join("|", delivery.Message.Headers.Span.ToArray().Select(h => h.Key + "=" + h.Value)));
            return receive.ProcessAsync(delivery, token);
        });
        await transport.StartAsync(default);
        try
        {
            await capture.WaitForProcesses(count).WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally { await transport.StopAsync(default); }
        var processes = capture.Activities.Where(a => a.OperationName == "process orders").ToArray();
        await Assert.That(processes.Select(a => a.Links.Single().Context.SpanId)).IsEquivalentTo(creates.Select(a => a.SpanId));
        await Assert.That(processes.All(a => a.TraceId == requestTrace && creates.Any(c => c.SpanId == a.ParentSpanId))).IsTrue();
        await Assert.That(receivedHeaders).IsEquivalentTo(persistedHeaders);
        await Assert.That(receivedSequences).IsEquivalentTo(rows.Select(row => mode == "ordered" ? row.InsertSequence : null));
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Pending_counts_only_committed_rows_for_both_transaction_integrations(bool ado, bool synchronous)
    {
        await using var database = new PostgreSqlFixture();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var capture = new Capture(tracing, instrumentation);
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton(instrumentation);
        if (ado) services.AddRelayerPostgreSqlOutbox();
        else
        {
            services.AddRelayerEntityFrameworkCoreOutbox();
            services.AddDbContext<Context>((provider, builder) => builder.UseNpgsql(database.ConnectionString).AddRelayerOutbox(provider));
        }
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        for (var commit = 0; commit < 2; commit++)
        {
            var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
            var context = ado ? null : scope.ServiceProvider.GetRequiredService<Context>();
            var ef = context is null ? null : await context.Database.BeginTransactionAsync();
            await using var transaction = ado
                ? await scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>().BeginTransactionAsync(source) : null;
            try
            {
                await enlistment.EnlistAsync(new(Guid.CreateVersion7(), "local", "outbox.trace;v=1",
                    new OutgoingMessage("orders", new byte[] { 1 }), DateTimeOffset.UtcNow), default);
                if (context is not null) await context.SaveChangesAsync();
                await Assert.That(capture.Pending.Sum()).IsEqualTo(0);
                if (commit == 0)
                {
                    if (ado) { if (synchronous) transaction!.Rollback(); else await transaction!.RollbackAsync(); }
                    else { if (synchronous) ef!.Rollback(); else await ef!.RollbackAsync(); }
                }
                else
                {
                    if (ado) { if (synchronous) transaction!.Commit(); else await transaction!.CommitAsync(); }
                    else { if (synchronous) ef!.Commit(); else await ef!.CommitAsync(); }
                }
            }
            finally { if (ef is not null) await ef.DisposeAsync(); }
        }
        await Assert.That(capture.Pending.Sum()).IsEqualTo(1);
        await Assert.That(capture.Sent).IsEmpty();
    }

    [Test]
    [Arguments(false, "missing")]
    [Arguments(true, "missing")]
    [Arguments(false, "invalid")]
    [Arguments(true, "invalid")]
    [Arguments(false, "external")]
    [Arguments(true, "external")]
    public async Task Dispatch_never_creates_or_rewrites_context_even_for_legacy_rows(bool batch, string context)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        using var capture = new Capture(tracing);
        var headers = context == "missing" ? Array.Empty<KeyValuePair<string, WireString>>() :
            new KeyValuePair<string, WireString>[] { new("traceparent", new(context == "invalid" ? "invalid" :
                "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")), new("tracestate", new("vendor=value")) };
        var buffered = new BufferedOutboxMessage();
        buffered.Copy(new(Guid.CreateVersion7(), "local", "outbox.trace;v=1", new("orders", new byte[] { 1 }, headers), DateTimeOffset.UtcNow), 1);
        var message = buffered.ToMessage();
        var expected = Snapshot(message);
        var seen = "";
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" },
            (sent, _) => { seen = Snapshot(sent); return default; },
            (sent, _) => { seen = Snapshot(sent.Span[0]); return default; });
        if (batch) await send.SendBatchAsync(new[] { message }); else await send.SendAsync(message);
        var activity = capture.Activities.Single();
        await Assert.That(activity.OperationName).IsEqualTo("send orders");
        await Assert.That(activity.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(seen).IsEqualTo(expected);
        await Assert.That(activity.Links.Count()).IsEqualTo(context == "external" ? 1 : 0);
    }

    [Test]
    [Arguments("send")]
    [Arguments("completion")]
    [Arguments("claim")]
    public async Task Failed_dispatch_keeps_pending_and_retry_preserves_context(string failure)
    {
        await using var database = new PostgreSqlFixture();
        await database.StartAsync();
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var capture = new Capture(tracing, instrumentation);
        var transport = new FailingTransport(tracing) { Fail = failure == "send" };
        var services = new ServiceCollection();
        services.AddSingleton(instrumentation);
        services.AddRelayerPostgreSqlOutbox();
        await using var provider = services.BuildServiceProvider();
        await using (var scope = provider.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
            await using var transaction = await outbox.BeginTransactionAsync(source);
            await outbox.EnlistAsync(new(Guid.CreateVersion7(), "local", "outbox.trace;v=1",
                new("orders", new byte[] { 1 }, new KeyValuePair<string, WireString>[]
                {
                    new("traceparent", new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")),
                    new("tracestate", new WireString("vendor=value")),
                }), DateTimeOffset.UtcNow.AddMinutes(-1)), default);
            await transaction.CommitAsync();
        }
        var store = new PostgreSqlOutboxStore(source);
        var rows = new OutboxRecord[1];
        await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
        if (failure == "claim")
            await store.TryClaimAsync(new(rows[0].Id, Guid.NewGuid()), DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddMinutes(1), default);
        if (failure == "completion")
        {
            await using var command = source.CreateCommand("""
                CREATE FUNCTION fail_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'completion failed'; END; $$;
                CREATE TRIGGER fail_delete BEFORE DELETE ON relayer_outbox FOR EACH ROW EXECUTE FUNCTION fail_delete();
                """);
            await command.ExecuteNonQueryAsync();
        }
        using var worker = new PostgreSqlDirectDispatcher(source, new(), new(), [transport],
            NullLogger<PostgreSqlDirectDispatcher>.Instance, instrumentation: instrumentation);
        await worker.DispatchRecoveredAsync(rows, default);
        await Assert.That(capture.Pending.Sum()).IsEqualTo(1);
        await Assert.That(capture.Sent.Sum()).IsEqualTo(failure == "claim" ? 0 : 1);
        await Assert.That(capture.Lags.Count).IsEqualTo(failure == "claim" ? 0 : 1);
        if (failure == "send")
        {
            var activity = capture.Activities.Single();
            await Assert.That(activity.Status).IsEqualTo(ActivityStatusCode.Error);
            await Assert.That(activity.GetTagItem("error.type")).IsEqualTo("_OTHER");
        }
        // Explicitly expire claims to simulate later recovery, without waiting for wall-clock time.
        await using (var command = source.CreateCommand("""
            DROP TRIGGER IF EXISTS fail_delete ON relayer_outbox;
            UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE claim_token IS NOT NULL;
            """)) await command.ExecuteNonQueryAsync();
        transport.Fail = false;
        await worker.DispatchRecoveredAsync(rows, default);
        await Assert.That(capture.Pending.Sum()).IsEqualTo(0);
        await Assert.That(capture.Sent.Sum()).IsEqualTo(failure == "claim" ? 1 : 2);
        await Assert.That(transport.Headers.All(headers => headers == Snapshot(rows[0].Message))).IsTrue();
        await Assert.That(capture.Lags.All(lag => lag >= 60)).IsTrue();
    }

    [Test]
    public async Task Partial_claim_compaction_keeps_original_enqueue_times_with_live_messages()
    {
        var first = DateTimeOffset.UtcNow.AddHours(-1);
        var second = first.AddMinutes(1);
        var third = first.AddMinutes(2);
        var messages = new OutgoingMessage[] { new("first", default), new("second", default), new("third", default) };
        var claims = new OutboxClaim[] { new(Guid.NewGuid(), Guid.NewGuid()), new(Guid.NewGuid(), Guid.NewGuid()), new(Guid.NewGuid(), Guid.NewGuid()) };
        var times = new[] { first, second, third };
        var count = PostgreSqlDirectDispatcher.CompactClaims(messages, claims, new[] { false, true, true }, times);
        await Assert.That(count).IsEqualTo(2);
        await Assert.That(messages[0].Destination).IsEqualTo("second");
        await Assert.That(messages[1].Destination).IsEqualTo("third");
        await Assert.That(times[0]).IsEqualTo(second);
        await Assert.That(times[1]).IsEqualTo(third);
    }

    private sealed class FailingTransport : ITransport, ITransportSendTracing
    {
        private readonly SendTracing _send;
        internal bool Fail;
        internal List<string> Headers { get; } = [];
        internal FailingTransport(MessageTracing tracing) => _send = new(tracing,
            new() { System = "test.native", Destination = "orders" }, SendNative, static (_, _) => default);
        public string Name => "local";
        public SendTracing GetSendTracing(string destination) => _send;
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => _send.SendAsync(message, cancellationToken);
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => _send.SendBatchAsync(messages, cancellationToken);
        private ValueTask SendNative(OutgoingMessage message, CancellationToken cancellationToken)
        {
            _send.RecordNativeSend();
            Headers.Add(Snapshot(message));
            return Fail ? ValueTask.FromException(new InvalidOperationException("native send failed")) : default;
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }

    private static string Snapshot(OutgoingMessage message) =>
        string.Join("|", message.Headers.Span.ToArray().Select(h => h.Key + "=" + h.Value));

    private static ActivityContext Read(OutgoingMessage message)
    {
        var parent = message.Headers.Span.ToArray().Single(h => h.Key == "traceparent").Value.ToString();
        ActivityContext.TryParse(parent, null, true, out var context);
        return context;
    }

    public sealed class Context(DbContextOptions<Context> options) : DbContext(options);
    [Message("outbox.trace")]
    public sealed record Probe(int Number);
    public sealed class Handler : IHandler<Probe>
    {
        public ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<T>(T message, IBufferWriter<byte> destination)
        {
            destination.GetSpan(1)[0] = (byte)((Probe)(object)message!).Number;
            destination.Advance(1);
        }
        public T Deserialize<T>(ReadOnlySequence<byte> source) => (T)(object)new Probe(source.FirstSpan[0]);
    }

    private sealed class Capture : IDisposable
    {
        private readonly ActivityListener _activities;
        private readonly MeterListener _metrics = new();
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _processed = new();
        private int _processCount;
        internal ConcurrentQueue<Activity> Activities { get; } = new();
        internal ConcurrentQueue<long> Pending { get; } = new();
        internal ConcurrentQueue<long> Sent { get; } = new();
        internal ConcurrentQueue<double> Lags { get; } = new();
        internal Capture(MessageTracing tracing, RelayerInstrumentation? instrumentation = null)
        {
            _activities = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    Activities.Enqueue(activity);
                    if (activity.OperationName == "process orders")
                    {
                        var count = Interlocked.Increment(ref _processCount);
                        _processed.GetOrAdd(count, static _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
                    }
                },
            };
            ActivitySource.AddActivityListener(_activities);
            _metrics.InstrumentPublished = (instrument, listener) =>
            {
                if (instrumentation is not null && ReferenceEquals(instrument.Meter.Scope, instrumentation))
                    listener.EnableMeasurementEvents(instrument);
            };
            _metrics.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
            {
                if (instrument.Name == "relayer.outbox.pending") Pending.Enqueue(value);
                if (instrument.Name == "messaging.client.sent.messages") Sent.Enqueue(value);
            });
            _metrics.SetMeasurementEventCallback<double>((instrument, value, _, _) =>
            {
                if (instrument.Name == "relayer.outbox.dispatch_lag") Lags.Enqueue(value);
            });
            _metrics.Start();
        }
        internal Task WaitForProcesses(int count) =>
            _processed.GetOrAdd(count, static _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        public void Dispose() { _activities.Dispose(); _metrics.Dispose(); }
    }
}
