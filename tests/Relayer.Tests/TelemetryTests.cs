#pragma warning disable RLY001 // Pipeline fixtures explicitly register their primitive wire identity and cached serializer.
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Relayer.Serialization;

namespace Relayer.Tests;

public class TelemetryTests
{
    [Test]
    public async Task Instruments_have_pinned_schema_units_types_and_bucket_advice()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new Capture(instrumentation);
        var expected = new Dictionary<string, (Type Type, string? Unit)>
        {
            ["messaging.client.operation.duration"] = (typeof(Histogram<double>), "s"),
            ["messaging.process.duration"] = (typeof(Histogram<double>), "s"),
            ["messaging.client.sent.messages"] = (typeof(Counter<long>), "{message}"),
            ["messaging.client.consumed.messages"] = (typeof(Counter<long>), "{message}"),
            ["relayer.critical_time"] = (typeof(Histogram<double>), "s"),
            ["relayer.retries"] = (typeof(Counter<long>), "{retry}"),
            ["relayer.dead_lettered"] = (typeof(Counter<long>), "{message}"),
            ["relayer.outbox.pending"] = (typeof(UpDownCounter<long>), "{message}"),
            ["relayer.outbox.dispatch_lag"] = (typeof(Histogram<double>), "s"),
            ["relayer.inbox.duplicates"] = (typeof(Counter<long>), "{message}"),
            ["relayer.consumer.lag"] = (typeof(Gauge<long>), "{message}"),
            ["relayer.circuit.state"] = (typeof(Gauge<int>), null),
        };
        await Assert.That(capture.Instruments.Count).IsEqualTo(expected.Count);
        foreach (var (name, (type, unit)) in expected)
        {
            var instrument = capture.Instruments[name];
            await Assert.That(instrument.GetType()).IsEqualTo(type);
            await Assert.That(instrument.Unit).IsEqualTo(unit);
            await Assert.That(instrument.Meter.Name).IsEqualTo("Relayer");
            await Assert.That(instrument.Meter.TelemetrySchemaUrl).IsEqualTo(RelayerTelemetry.SchemaUrl);
        }
        foreach (var name in new[] { "messaging.client.operation.duration", "messaging.process.duration" })
        {
            var histogram = (Histogram<double>)capture.Instruments[name];
            await Assert.That(histogram.Advice!.HistogramBucketBoundaries!.SequenceEqual(
                new double[] { .005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10 })).IsTrue();
        }
    }

    [Test]
    public async Task Recorders_preserve_units_tags_and_error_isolation()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new Capture(instrumentation);
        var process = instrumentation.CreateEndpoint(Options());
        var send = instrumentation.CreateEndpoint(Options("send"));
        var settle = instrumentation.CreateEndpoint(Options("ack"));
        send.RecordSent(3, failed: true);
        send.RecordSent(2);
        send.RecordDuration(TimeSpan.FromMilliseconds(250), failed: true);
        settle.RecordDuration(TimeSpan.FromSeconds(2));
        process.RecordDuration(TimeSpan.FromSeconds(3));
        process.RecordConsumed();
        process.RecordCriticalTime(TimeSpan.FromSeconds(4));
        process.RecordRetry(2);
        process.RecordDeadLettered();
        send.AddOutboxPending(5);
        send.AddOutboxPending(-2);
        send.RecordDispatchLag(TimeSpan.FromSeconds(6));
        process.RecordInboxDuplicate();
        process.RecordConsumerLag(7);
        process.RecordCircuitState(1);

        var values = capture.Values.ToArray();
        foreach (var value in values)
        {
            await Assert.That(value.Tags["messaging.system"]).IsEqualTo("kafka");
            await Assert.That(value.Tags["messaging.operation.name"]).IsNotNull();
            await Assert.That(value.Tags["messaging.destination.name"]).IsEqualTo("orders");
            await Assert.That(value.Tags["messaging.consumer.group.name"]).IsEqualTo("billing");
            await Assert.That(value.Tags["messaging.destination.subscription.name"]).IsEqualTo("subscription");
            await Assert.That(value.Tags["server.address"]).IsEqualTo("broker");
            await Assert.That(value.Tags["server.port"]).IsEqualTo(9092);
            await Assert.That(value.Tags.Keys.Any(key => key.Contains("message.id") || key.Contains("key"))).IsFalse();
        }
        await Assert.That(values[0].Number).IsEqualTo(3d);
        await Assert.That(values[0].Tags["error.type"]).IsEqualTo("_OTHER");
        await Assert.That(values[1].Tags.ContainsKey("error.type")).IsFalse();
        await Assert.That(values[2].Number).IsEqualTo(.25);
        await Assert.That(values[3].Tags["messaging.operation.name"]).IsEqualTo("ack");
        await Assert.That(values.Single(v => v.Name == "messaging.process.duration").Number).IsEqualTo(3d);
        await Assert.That(values.Where(v => v.Name == "relayer.outbox.pending").Sum(v => v.Number)).IsEqualTo(3d);
        await Assert.That(values.Single(v => v.Name == "relayer.outbox.dispatch_lag").Number).IsEqualTo(6d);
        await Assert.That(values.Single(v => v.Name == "relayer.consumer.lag").Number).IsEqualTo(7d);
    }

    [Test]
    public async Task Processing_includes_async_settlement_and_counts_one_delivery()
    {
        await using var provider = Provider();
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        using var capture = new Capture(instrumentation);
        var settled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pipeline = Builder(provider).Telemetry(instrumentation.CreateEndpoint(Options())).Settle(context =>
        {
            if (context.Features.Get<NativeMessage<Incoming>>()?.Message is null) throw new InvalidOperationException("Native delivery lost.");
            entered.SetResult();
            return new(settled.Task);
        }).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new HeaderNames("cloudEvents:"), 1);
        var pending = receive.ProcessAsync(new Incoming());
        await entered.Task;
        await Assert.That(capture.Values.Count(v => v.Name == "messaging.client.consumed.messages")).IsEqualTo(1);
        await Assert.That(capture.Values.Any(v => v.Name == "messaging.process.duration")).IsFalse();
        settled.SetResult();
        await pending;
        await Assert.That(capture.Values.Count(v => v.Name == "messaging.process.duration")).IsEqualTo(1);
        await Assert.That(capture.Values.Single(v => v.Name == "messaging.process.duration").Number).IsGreaterThan(0d);
        await Assert.That(capture.Values.Single(v => v.Name == "relayer.critical_time").Number).IsGreaterThanOrEqualTo(60d);
    }

    [Test]
    public async Task Handler_failure_logs_scoped_identity_and_trace_even_when_failure_handling_recovers()
    {
        var logger = new Logger();
        using var instrumentation = new RelayerInstrumentation(new LoggerFactory(logger));
        using var capture = new Capture(instrumentation);
        await using var provider = Provider();
        var error = new InvalidOperationException("broken handler");
        provider.GetRequiredService<Control>().Handle = _ => ValueTask.FromException(error);
        var settlements = 0;
        var pipeline = Builder(provider).Telemetry(instrumentation.CreateEndpoint(Options()))
            .Settle(_ => { settlements++; return default; }).Build(1);
        using var activity = new Activity("test").Start();
        using var context = pipeline.RentContext(Message() with { Headers = new Headers() });
        await pipeline.ProcessAsync(context);
        var entry = logger.Entries.Single();
        await Assert.That(entry.Exception).IsSameReferenceAs(error);
        await Assert.That(entry.Scope["MessageId"]).IsEqualTo("delivery");
        await Assert.That(entry.Scope["MessageType"]).IsEqualTo("number;v=1");
        await Assert.That(entry.Scope["CorrelationId"]).IsEqualTo("conversation");
        await Assert.That(entry.State["TraceId"]).IsEqualTo(activity.TraceId.ToString());
        await Assert.That(entry.State["SpanId"]).IsEqualTo(activity.SpanId.ToString());
        await Assert.That(settlements).IsEqualTo(1);
        await Assert.That(capture.Values.Single(v => v.Name == "messaging.process.duration").Tags["error.type"]).IsEqualTo("_OTHER");
        await Assert.That(logger.CurrentScope).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Settlement_is_skipped_after_failed_durability_and_settlement_errors_are_not_reclassified(bool failSettlement)
    {
        await using var provider = Provider();
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        using var capture = new Capture(instrumentation);
        var error = new ApplicationException("durability failed");
        var failures = 0;
        var settlements = 0;
        if (!failSettlement) provider.GetRequiredService<Control>().Handle = _ => ValueTask.FromException(new Exception("handler"));
        var pipeline = Builder(provider, (_, _, _) => { failures++; return ValueTask.FromException(error); })
            .Telemetry(instrumentation.CreateEndpoint(Options()))
            .Settle(_ => { settlements++; return ValueTask.FromException(error); }).Build();
        using var context = pipeline.RentContext(Message());
        await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<ApplicationException>();
        await Assert.That(failures).IsEqualTo(failSettlement ? 0 : 1);
        await Assert.That(settlements).IsEqualTo(failSettlement ? 1 : 0);
        await Assert.That(capture.Values.Single(v => v.Name == "messaging.process.duration").Tags["error.type"]).IsEqualTo("_OTHER");
    }

    [Test]
    public async Task Disabled_listeners_and_logging_do_not_read_optional_headers_and_context_reuse_clears_scope()
    {
        await using var provider = Provider();
        var instrumentation = provider.GetRequiredService<RelayerInstrumentation>();
        var pipeline = Builder(provider).Telemetry(instrumentation.CreateEndpoint(Options())).Build(1);
        for (var i = 0; i < 2; i++)
        {
            using var context = pipeline.RentContext(Message() with { Headers = new UnreadHeaders() });
            await pipeline.ProcessAsync(context);
            await Assert.That(context.LogScope[0].Value).IsNull();
        }
    }

    [Test]
    public async Task Malformed_envelopes_still_log_and_count_delivery_without_logging_bypassing_failure_handling()
    {
        var logger = new Logger();
        using var instrumentation = new RelayerInstrumentation(new LoggerFactory(logger));
        using var capture = new Capture(instrumentation);
        await using var provider = Provider();
        var failures = 0;
        var pipeline = Builder(provider, (_, _, _) => { failures++; return default; })
            .Telemetry(instrumentation.CreateEndpoint(Options())).Build();
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new HeaderNames("cloudEvents:"));
        await receive.ProcessAsync(new Incoming { Type = null });
        await Assert.That(failures).IsEqualTo(1);
        await Assert.That(logger.Entries.Single().Exception is MessageEnvelopeException).IsTrue();
        await Assert.That(capture.Values.Count(v => v.Name == "messaging.client.consumed.messages")).IsEqualTo(1);
    }

    [Test]
    [Arguments("before")]
    [Arguments("handler")]
    [Arguments("settlement")]
    public async Task Delivery_cancellation_is_counted_without_error_logging(string stage)
    {
        var logger = new Logger();
        using var instrumentation = new RelayerInstrumentation(new LoggerFactory(logger));
        await using var provider = Provider();
        using var capture = new Capture(instrumentation);
        using var cancellation = new CancellationTokenSource();
        provider.GetRequiredService<Control>().Handle = context =>
        {
            if (stage == "handler")
            {
                cancellation.Cancel();
                context.CancellationToken.ThrowIfCancellationRequested();
            }
            return default;
        };
        var settlements = 0;
        var failures = 0;
        var pipeline = Builder(provider, (_, _, _) => { failures++; return default; })
            .Telemetry(instrumentation.CreateEndpoint(Options()))
            .Settle(context =>
            {
                settlements++;
                cancellation.Cancel();
                context.CancellationToken.ThrowIfCancellationRequested();
                return default;
            }).Build();
        if (stage == "before") cancellation.Cancel();
        using var context = pipeline.RentContext(Message(), cancellation.Token);
        await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<OperationCanceledException>();
        await Assert.That(settlements).IsEqualTo(stage == "settlement" ? 1 : 0);
        await Assert.That(failures).IsEqualTo(0);
        await Assert.That(logger.Entries.Count).IsEqualTo(0);
        await Assert.That(logger.CurrentScope).IsNull();
        await Assert.That(capture.Values.Count(v => v.Name == "messaging.client.consumed.messages")).IsEqualTo(1);
        await Assert.That(capture.Values.Single(v => v.Name == "messaging.process.duration").Tags["error.type"]).IsEqualTo("_OTHER");
    }

    [Test]
    public async Task Cancellation_without_delivery_cancellation_is_logged_and_classified()
    {
        var logger = new Logger();
        using var instrumentation = new RelayerInstrumentation(new LoggerFactory(logger));
        await using var provider = Provider();
        var error = new OperationCanceledException("Handler's independent operation was canceled.");
        provider.GetRequiredService<Control>().Handle = _ => ValueTask.FromException(error);
        var failures = 0;
        var pipeline = Builder(provider, (_, _, _) => { failures++; return default; })
            .Telemetry(instrumentation.CreateEndpoint(Options())).Build();
        using var context = pipeline.RentContext(Message());
        await pipeline.ProcessAsync(context);
        await Assert.That(failures).IsEqualTo(1);
        await Assert.That(logger.Entries.Single().Exception).IsSameReferenceAs(error);
    }

    [Test]
    public async Task Async_scope_disposal_finishes_before_settlement_and_its_services_are_unavailable()
    {
        var gate = new DisposalGate();
        await using var provider = Provider(services => services.AddScoped(_ => gate));
        provider.GetRequiredService<Control>().Handle = context =>
        {
            _ = context.Services.GetRequiredService<DisposalGate>();
            return default;
        };
        var settlements = 0;
        var pipeline = Builder(provider).Settle(context =>
        {
            try { _ = context.Services; }
            catch (InvalidOperationException) { settlements++; return default; }
            throw new InvalidOperationException("Disposed scope is still exposed.");
        }).Build();
        using var context = pipeline.RentContext(Message());
        var pending = pipeline.ProcessAsync(context);
        await gate.Entered.Task;
        await Assert.That(settlements).IsEqualTo(0);
        gate.Finished.SetResult();
        await pending;
        await Assert.That(settlements).IsEqualTo(1);
    }

    [Test]
    public async Task Endpoint_configuration_fails_early_and_instrumentation_is_owned_by_each_provider()
    {
        await using var first = Provider();
        await using var second = Provider();
        var instrumentation = first.GetRequiredService<RelayerInstrumentation>();
        await Assert.That(instrumentation).IsSameReferenceAs(first.GetRequiredService<RelayerInstrumentation>());
        await Assert.That(instrumentation).IsNotSameReferenceAs(second.GetRequiredService<RelayerInstrumentation>());
        await Assert.That(() => instrumentation.CreateEndpoint(new() { System = " " })).Throws<ArgumentException>();
        await Assert.That(() => instrumentation.CreateEndpoint(new() { System = "kafka", ServerPort = 65536 })).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => Builder(first).Telemetry(instrumentation.CreateEndpoint(Options("send")))).Throws<ArgumentException>();
    }

    private static EndpointTelemetryOptions Options(string operation = "process") => new()
    {
        System = "kafka", OperationName = operation, Destination = "orders", ConsumerGroup = "billing",
        Subscription = "subscription", ServerAddress = "broker", ServerPort = 9092,
    };

    private static ServiceProvider Provider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddSingleton<Handler>();
        services.AddRelayer(r => { r.MessageTypes.Add<int>("number"); r.Handler<Handler>(); });
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static ConsumePipelineBuilder Builder(ServiceProvider provider, ConsumeFailureDelegate? failure = null) =>
        new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(), provider.GetRequiredService<MessageTypeRegistry>(),
            new Serializer(), failure ?? (static (_, _, _) => default))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<int, Handler>());

    private static ConsumeMessage Message() => new() { MessageId = "delivery", MessageType = new("number", 1), Body = default };
    public sealed class Control { public ConsumeDelegate? Handle { get; set; } }
    public sealed class Handler(Control control) : IHandler<int>
    {
        public ValueTask HandleAsync(int message, MessageContext context, CancellationToken cancellationToken) => control.Handle?.Invoke(context) ?? default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "test/cached";
        public T Deserialize<T>(ReadOnlySequence<byte> source) => (T)(object)1;
        public void Serialize<T>(T message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    private sealed class Headers : MessageHeaders
    {
        public override bool TryGetValue(string name, out WireString value)
        {
            value = name == "correlation-id" ? new WireString("conversation") : default;
            return !value.IsEmpty;
        }
    }
    private sealed class UnreadHeaders : MessageHeaders
    {
        public override bool TryGetValue(string name, out WireString value) => throw new InvalidOperationException("Optional headers were read.");
    }
    private sealed class DisposalGate : IAsyncDisposable
    {
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Finished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ValueTask DisposeAsync()
        {
            Entered.SetResult();
            return new(Finished.Task);
        }
    }
    private sealed class Incoming
    {
        public string? Type { get; init; } = "number;v=1";
        public string Time { get; } = DateTimeOffset.UtcNow.AddMinutes(-1).ToString("O");
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => default;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch
            {
                "cloudEvents:type" => message.Type is { } type ? new(type) : default,
                "cloudEvents:id" => new("native"),
                "cloudEvents:time" => new(message.Time),
                _ => default,
            };
            return !value.IsEmpty;
        }
    }

    private sealed record Measurement(string Name, double Number, Dictionary<string, object?> Tags);
    private sealed class Capture : IDisposable
    {
        private readonly MeterListener _listener = new();
        internal Dictionary<string, Instrument> Instruments { get; } = [];
        internal ConcurrentQueue<Measurement> Values { get; } = new();
        internal Capture(RelayerInstrumentation instrumentation)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (!ReferenceEquals(instrument.Meter.Scope, instrumentation)) return;
                Instruments.Add(instrument.Name, instrument);
                listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Add(instrument, value, tags));
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument, value, tags));
            _listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Add(instrument, value, tags));
            _listener.Start();
        }
        private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags) =>
            Values.Enqueue(new(instrument.Name, value, tags.ToArray().ToDictionary()));
        public void Dispose() => _listener.Dispose();
    }

    private sealed record Entry(Exception? Exception, Dictionary<string, object?> Scope, Dictionary<string, object?> State);
    private sealed class Logger : ILogger
    {
        internal IReadOnlyList<KeyValuePair<string, object?>>? CurrentScope { get; private set; }
        internal List<Entry> Entries { get; } = [];
        public bool IsEnabled(LogLevel logLevel) => true;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            CurrentScope = (IReadOnlyList<KeyValuePair<string, object?>>)(object)state;
            return new Scope(this);
        }
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? error, Func<TState, Exception?, string> formatter) =>
            Entries.Add(new(error, CurrentScope!.ToDictionary(), ((IEnumerable<KeyValuePair<string, object?>>)(object)state!).ToDictionary()));
        private sealed class Scope(Logger owner) : IDisposable { public void Dispose() => owner.CurrentScope = null; }
    }
    private sealed class LoggerFactory(Logger logger) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => logger;
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public void Dispose() { }
    }
}
