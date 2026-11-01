using System.Buffers;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.InMemory;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Tests;

public class SendTracingTests
{
    [Test]
    public async Task Direct_send_captures_producer_before_serialization_and_restores_ambient()
    {
        using var fixture = new Fixture(new() { IncludeBodySize = true });
        using var ambient = new Activity("request").Start();
        await fixture.Sender.SendNowAsync(fixture.Message);
        var send = fixture.Stopped.Single();
        await Assert.That(send.Kind).IsEqualTo(ActivityKind.Producer);
        await Assert.That(send.ParentSpanId).IsEqualTo(ambient.SpanId);
        await Assert.That(fixture.Serializer.Current).IsSameReferenceAs(send);
        await Assert.That(fixture.Sink.Context.SpanId).IsEqualTo(send.SpanId);
        await Assert.That(send.DisplayName).IsEqualTo("send orders");
        await Assert.That(send.GetTagItem("messaging.system")).IsEqualTo("test.native");
        await Assert.That(send.GetTagItem("server.address")).IsEqualTo("broker");
        await Assert.That(send.GetTagItem("messaging.message.body.size")).IsEqualTo(1);
        await Assert.That(send.GetTagItem("cloudevents.event_type")).IsEqualTo("send.probe;v=1");
        await Assert.That(send.GetTagItem("messaging.batch.message_count")).IsNull();
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
    }

    [Test]
    public async Task Pending_send_keeps_span_open_without_leaking_ambient_and_returns_pool_clean()
    {
        using var fixture = new Fixture();
        using var ambient = new Activity("request").Start();
        fixture.Sink.Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = fixture.Sender.SendNowAsync(fixture.Message);
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(fixture.Stopped).IsEmpty();
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
        fixture.Sink.Completion.SetResult();
        await pending;
        await Assert.That(fixture.Stopped.Count).IsEqualTo(1);
        fixture.Sink.Completion = null;
        await fixture.Sender.SendNowAsync(fixture.Message);
        await Assert.That(fixture.Stopped.Count).IsEqualTo(2);
        await Assert.That(fixture.Stopped[0].SpanId).IsNotEqualTo(fixture.Stopped[1].SpanId);
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
    }

    [Test]
    public async Task Raw_external_creation_context_is_preserved_and_linked()
    {
        using var fixture = new Fixture(raw: true);
        var parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        await fixture.Sender.SendNowAsync(fixture.Message, options =>
        {
            options.Headers["traceparent"] = parent;
            options.Headers["tracestate"] = "vendor=value";
        });
        var send = fixture.Stopped.Single();
        await Assert.That(send.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(fixture.Sink.Headers["traceparent"]).IsEqualTo(parent);
        await Assert.That(fixture.Sink.Headers["tracestate"]).IsEqualTo("vendor=value");
        await Assert.That(send.Links.Single().Context.SpanId).IsEqualTo(fixture.Sink.Context.SpanId);
    }

    [Test]
    public async Task Raw_context_set_by_middleware_is_linked_before_serialization()
    {
        const string parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        using var fixture = new Fixture(raw: true, middleware: options => options.Headers["traceparent"] = parent);
        await fixture.Sender.SendNowAsync(fixture.Message);
        var send = fixture.Stopped.Single();
        await Assert.That(send.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(send.Links.Single().Context.SpanId).IsEqualTo(fixture.Sink.Context.SpanId);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Serialization_and_native_failures_are_tagged_logged_and_do_not_leak(bool serialization)
    {
        using var fixture = new Fixture();
        using var ambient = new Activity("request").Start();
        if (serialization) fixture.Serializer.Error = new InvalidOperationException("serialize failed");
        else fixture.Sink.Error = new InvalidOperationException("native failed");
        await Assert.That(async () => await fixture.Sender.SendNowAsync(fixture.Message)).Throws<InvalidOperationException>();
        var send = fixture.Stopped.Single();
        await Assert.That(send.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(send.GetTagItem("error.type")).IsEqualTo("_OTHER");
        await Assert.That(send.Events).IsEmpty();
        await Assert.That(fixture.Logger.Error).IsSameReferenceAs(serialization ? fixture.Serializer.Error : fixture.Sink.Error);
        await Assert.That(fixture.Logger.Message).Contains(send.TraceId.ToString());
        await Assert.That(fixture.Logger.Message).Contains(send.SpanId.ToString());
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Native_batch_links_create_spans_or_shares_batch_producer_context(bool createSpans)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions { CreateSpansForBatches = createSpans }), []);
        var stopped = new List<Activity>();
        using var listener = Listen(tracing, stopped);
        await using var transport = NativeTransport(tracing);
        var contexts = new List<ActivityContext>();
        var options = new SendOptions().ConfigureNative<InMemoryMessage>(message => contexts.Add(Read(message.Headers)));
        var messages = new[] { new OutgoingMessage("orders", new byte[] { 1 }, options: options), new("orders", new byte[] { 2 }, options: options) };
        using var ambient = new Activity("request").Start();
        await transport.SendBatchAsync(messages, default);
        var send = stopped.Single(activity => activity.OperationName == "send orders");
        await Assert.That(send.GetTagItem("messaging.batch.message_count")).IsEqualTo(2);
        await Assert.That(send.GetTagItem("messaging.message.id")).IsNull();
        await Assert.That(send.ParentSpanId).IsEqualTo(ambient.SpanId);
        await Assert.That(send.Kind).IsEqualTo(createSpans ? ActivityKind.Client : ActivityKind.Producer);
        await Assert.That(stopped.Count).IsEqualTo(createSpans ? 3 : 1);
        if (createSpans)
        {
            await Assert.That(contexts.Select(context => context.SpanId)).IsEquivalentTo(send.Links.Select(link => link.Context.SpanId));
            await Assert.That(contexts[0].SpanId).IsNotEqualTo(contexts[1].SpanId);
        }
        else
        {
            await Assert.That(contexts.All(context => context.SpanId == send.SpanId)).IsTrue();
            await Assert.That(send.Links).IsEmpty();
        }
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
    }

    [Test]
    public async Task Native_core_copy_preserves_byte_headers_and_links_original_context()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var stopped = new List<Activity>();
        using var listener = Listen(tracing, stopped);
        await using var transport = NativeTransport(tracing);
        var parent = Encoding.UTF8.GetBytes("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01");
        var state = Encoding.UTF8.GetBytes("vendor=value");
        var headers = new[] { new KeyValuePair<string, WireString>("traceparent", new(parent)), new("tracestate", new WireString(state)), new("ce_id", new WireString("original-id")) };
        var seen = new Dictionary<string, string>();
        var options = new SendOptions().ConfigureNative<InMemoryMessage>(native =>
        {
            foreach (var header in native.Headers.Span) seen[header.Key] = header.Value.ToString();
        });
        await transport.SendAsync(new("orders", new byte[] { 1 }, headers, options: options), default);
        var send = stopped.Single();
        await Assert.That(send.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(seen["ce_id"]).IsEqualTo("original-id");
        await Assert.That(seen["traceparent"]).IsEqualTo(Encoding.UTF8.GetString(parent));
        await Assert.That(seen["tracestate"]).IsEqualTo("vendor=value");
        await Assert.That(send.Links.Single().Context.TraceState).IsEqualTo("vendor=value");
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Batch_preserves_external_context_and_clears_links_between_reuses(bool createSpans)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions { CreateSpansForBatches = createSpans }), []);
        var stopped = new List<Activity>();
        using var listener = Listen(tracing, stopped);
        await using var transport = NativeTransport(tracing);
        const string parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        var headers = new[] { new KeyValuePair<string, WireString>("traceparent", new(parent)) };
        var contexts = new List<ActivityContext>();
        var options = new SendOptions().ConfigureNative<InMemoryMessage>(native => contexts.Add(Read(native.Headers)));
        await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 1 }, headers, options: options), new("orders", new byte[] { 2 }, options: options) }, default);
        var send = stopped.Single(activity => activity.OperationName == "send orders");
        await Assert.That(send.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(send.Links.First().Context.SpanId).IsEqualTo(contexts[0].SpanId);
        await Assert.That(contexts[0].SpanId.ToString()).IsEqualTo("0123456789abcdef");
        await Assert.That(stopped.Count).IsEqualTo(createSpans ? 2 : 1);
        stopped.Clear();
        await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 3 }, options: options) }, default);
        var next = stopped.Single(activity => activity.OperationName == "send orders");
        await Assert.That(next.Links.Any(link => link.Context.SpanId == contexts[0].SpanId)).IsFalse();
    }

    [Test]
    public async Task Native_cancellation_tags_pending_send_and_restores_ambient()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var stopped = new List<Activity>();
        using var listener = Listen(tracing, stopped);
        await using var transport = new InMemoryTransport("local", new Dictionary<string, InMemoryEndpointOptions>
            { ["orders"] = new() { Capacity = 1 } }, tracing: tracing);
        await transport.SendAsync(new("orders", new byte[] { 1 }), default);
        using var ambient = new Activity("request").Start();
        using var cancellation = new CancellationTokenSource();
        var pending = transport.SendAsync(new("orders", new byte[] { 2 }), cancellation.Token);
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
        cancellation.Cancel();
        await Assert.That(async () => await pending).Throws<OperationCanceledException>();
        await Assert.That(stopped.Last().Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
    }

    [Test]
    public async Task Native_batch_failure_logs_once_and_does_not_emit_single_send_spans()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var stopped = new List<Activity>();
        var logger = new Logger();
        using var listener = Listen(tracing, stopped);
        await using var transport = NativeTransport(tracing, logger);
        var options = new SendOptions().ConfigureNative<InMemoryMessage>(static _ => throw new InvalidOperationException("native failed"));
        await Assert.That(async () => await transport.SendBatchAsync(new[] { new OutgoingMessage("orders", new byte[] { 1 }, options: options) }, default))
            .Throws<InvalidOperationException>();
        var send = stopped.Single(activity => activity.OperationName == "send orders");
        await Assert.That(send.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(send.GetTagItem("error.type")).IsEqualTo("_OTHER");
        await Assert.That(logger.Count).IsEqualTo(1);
        await Assert.That(logger.Message).Contains(send.SpanId.ToString());
    }

    [Test]
    public async Task No_listener_direct_send_is_allocation_free_after_warmup()
    {
        using var fixture = new Fixture(listen: false);
        for (var i = 0; i < 100; i++) fixture.Sender.SendNowAsync(fixture.Message).GetAwaiter().GetResult();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) fixture.Sender.SendNowAsync(fixture.Message).GetAwaiter().GetResult();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    [Test]
    public async Task InMemory_registration_integrates_route_and_native_send_without_duplicate_spans()
    {
        var services = new ServiceCollection();
        var serializer = new ProbeSerializer();
        services.AddSingleton<IMessageSerializer>(serializer);
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Probe>("send.probe");
            builder.InMemory("local", local => local.Send<Probe>("orders"));
        });
        await using var provider = services.BuildServiceProvider();
        var stopped = new List<Activity>();
        using var listener = Listen(provider.GetRequiredService<MessageTracing>(), stopped);
        using var scope = provider.CreateScope();
        var context = default(ActivityContext);
        await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(), options =>
            options.ConfigureNative<InMemoryMessage>(native => context = Read(native.Headers)));
        await Assert.That(stopped.Count).IsEqualTo(1);
        await Assert.That(context.SpanId).IsEqualTo(stopped[0].SpanId);
        await Assert.That(serializer.Current).IsSameReferenceAs(stopped[0]);
        await Assert.That(stopped[0].GetTagItem("messaging.system")).IsEqualTo(InMemoryTransport.MessagingSystem);
    }

    [Test]
    public async Task Async_native_failure_finishes_and_tags_route_span()
    {
        using var fixture = new Fixture();
        fixture.Sink.Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = fixture.Sender.SendNowAsync(fixture.Message);
        fixture.Sink.Completion.SetException(new InvalidOperationException("async native failed"));
        await Assert.That(async () => await pending).Throws<InvalidOperationException>();
        await Assert.That(fixture.Stopped.Single().Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(fixture.Logger.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Native_no_listener_batch_is_allocation_free_after_warmup()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" },
            static (_, _) => default, static (_, _) => default);
        var messages = new[] { new OutgoingMessage("orders", new byte[] { 1 }), new("orders", new byte[] { 2 }) };
        for (var i = 0; i < 100; i++) send.SendBatchAsync(messages).GetAwaiter().GetResult();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) send.SendBatchAsync(messages).GetAwaiter().GetResult();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    private static InMemoryTransport NativeTransport(MessageTracing tracing, Logger? logger = null) =>
        new("local", new Dictionary<string, InMemoryEndpointOptions> { ["orders"] = new() }, tracing: tracing,
            loggerFactory: logger is null ? null : new LoggerFactory(logger));

    private static ActivityContext Read(ReadOnlyMemory<KeyValuePair<string, WireString>> headers)
    {
        string? parent = null, state = null;
        foreach (var header in headers.Span)
        {
            if (header.Key == "traceparent") parent = header.Value.ToString();
            if (header.Key == "tracestate") state = header.Value.ToString();
        }
        ActivityContext.TryParse(parent, state, true, out var context);
        return context;
    }

    private static ActivityListener Listen(MessageTracing tracing, List<Activity> stopped)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = stopped.Add,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private sealed class Fixture : IDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly IServiceScope _scope;
        private readonly ActivityListener? _listener;
        internal List<Activity> Stopped { get; } = [];
        internal ProbeSerializer Serializer { get; } = new();
        internal Logger Logger { get; } = new();
        internal Sink Sink { get; }
        internal ISender Sender { get; }
        internal Probe Message { get; } = new();

        internal Fixture(TracingOptions? options = null, bool raw = false, bool listen = true, Action<SendOptions>? middleware = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton<IMessageSerializer>(Serializer);
            services.AddSingleton(Options.Create(options ?? new()));
            services.AddSingleton<ITransport>(provider => new Sink(provider.GetRequiredService<MessageTracing>(), Logger, listen));
            if (middleware is not null) services.AddSingleton<ISendMiddleware>(new ConfigureMiddleware(middleware));
            services.AddRelayer(builder =>
            {
                builder.MessageTypes.Add<Probe>("send.probe");
                builder.AddTransport<EmptyOptions, EmptyOptions, EmptyOptions>("sink", transport =>
                    transport.Send<Probe>("orders", route => route.SendOptions = new() { EnvelopeMode = raw ? MessageEnvelopeMode.Raw : MessageEnvelopeMode.CloudEvents }));
            });
            _provider = services.BuildServiceProvider();
            var tracing = _provider.GetRequiredService<MessageTracing>();
            if (listen) _listener = Listen(tracing, Stopped);
            _scope = _provider.CreateScope();
            Sender = _scope.ServiceProvider.GetRequiredService<ISender>();
            Sink = (Sink)_provider.GetRequiredService<ITransport>();
        }

        public void Dispose() { _scope.Dispose(); _listener?.Dispose(); _provider.Dispose(); }
    }

    private sealed class Probe;
    private sealed class EmptyOptions;
    private sealed class ConfigureMiddleware(Action<SendOptions> configure) : ISendMiddleware
    {
        public ValueTask InvokeAsync(SendContext context, SendDelegate next) { configure(context.Options); return next(context); }
    }
    private sealed class ProbeSerializer : IMessageSerializer
    {
        internal Activity? Current;
        internal Exception? Error;
        public string ContentType => "test/probe";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            Current = Activity.Current;
            if (Error is not null) throw Error;
            destination.GetSpan(1)[0] = 42;
            destination.Advance(1);
        }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }

    private sealed class Sink : ITransport, ITransportSendTracing, IDisposable
    {
        private readonly SendTracing _tracing;
        private readonly bool _capture;
        internal ActivityContext Context;
        internal Dictionary<string, string> Headers { get; } = [];
        internal TaskCompletionSource? Completion;
        internal Exception? Error;
        internal Sink(MessageTracing tracing, ILogger logger, bool capture)
        {
            _capture = capture;
            _tracing = new(tracing, new() { System = "test.native", Destination = "orders", ServerAddress = "broker" }, SendCore, BatchCore, logger);
        }
        public string Name => "sink";
        public SendTracing GetSendTracing(string destination) => _tracing;
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => _tracing.SendAsync(message, cancellationToken);
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => _tracing.SendBatchAsync(messages, cancellationToken);
        private ValueTask SendCore(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (_capture)
            {
                Context = Read(message.Headers);
                Headers.Clear();
                foreach (var header in message.Headers.Span) Headers[header.Key] = header.Value.ToString();
            }
            if (Error is not null) throw Error;
            return Completion is null ? default : new(Completion.Task);
        }
        private ValueTask BatchCore(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => default;
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
        public void Dispose() { }
    }

    private sealed class Logger : ILogger
    {
        internal Exception? Error;
        internal string Message = "";
        internal int Count;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        { Error = exception; Message = formatter(state, exception); Count++; }
    }

    private sealed class LoggerFactory(Logger logger) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => logger;
        public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();
        public void Dispose() { }
    }
}
