using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Relayer.Tests;

public class TracingTests
{
    private static readonly MessagingEndpoint Endpoint = new()
    {
        System = "kafka", Destination = "orders", ServerAddress = "broker", ServerPort = 9092,
        ConsumerGroup = "billing", ClientId = "client",
    };

    [Test]
    public async Task Source_is_provider_owned_and_publishes_pinned_schema()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.Telemetry());
        using var first = services.BuildServiceProvider();
        using var second = services.BuildServiceProvider();
        var one = first.GetRequiredService<MessageTracing>();
        var two = second.GetRequiredService<MessageTracing>();
        await Assert.That(one.Source.Name).IsEqualTo("Relayer");
        await Assert.That(one.Source.TelemetrySchemaUrl).IsEqualTo(RelayerTelemetry.SchemaUrl);
        await Assert.That(ReferenceEquals(one.Source, two.Source)).IsFalse();
        await Assert.That(ReferenceEquals(one, first.GetRequiredService<MessageTracing>())).IsTrue();
    }

    [Test]
    [Arguments(MessagingOperation.Send, ActivityKind.Producer, "send")]
    [Arguments(MessagingOperation.Create, ActivityKind.Producer, "create")]
    [Arguments(MessagingOperation.Process, ActivityKind.Consumer, "process")]
    [Arguments(MessagingOperation.Settle, ActivityKind.Client, "commit")]
    public async Task Operations_have_native_names_kinds_and_sampling_attributes(MessagingOperation operation, ActivityKind kind, string name)
    {
        using var capture = new Capture();
        var endpoint = new MessageOperationTracing(capture.Tracing, Endpoint, operation,
            operation == MessagingOperation.Settle ? name : null);
        using var activity = endpoint.Start(new() { MessageId = "id", EventType = "orders;v=1", EventSource = "/shop", PartitionId = "3" });
        await Assert.That(activity!.DisplayName).IsEqualTo(name + " orders");
        await Assert.That(activity.Kind).IsEqualTo(kind);
        await Assert.That(capture.SampleTags["messaging.system"]).IsEqualTo("kafka");
        await Assert.That(capture.SampleTags["messaging.operation.name"]).IsEqualTo(name);
        await Assert.That(capture.SampleTags["messaging.operation.type"]).IsEqualTo(operation.ToString().ToLowerInvariant());
        await Assert.That(capture.SampleTags["messaging.destination.partition.id"]).IsEqualTo("3");
        await Assert.That(capture.SampleTags["server.port"]).IsEqualTo(9092);
        await Assert.That(activity.GetTagItem("messaging.message.id")).IsEqualTo("id");
        await Assert.That(activity.GetTagItem("cloudevents.event_type")).IsEqualTo("orders;v=1");
        await Assert.That(activity.GetTagItem("messaging.batch.message_count")).IsNull();
    }

    [Test]
    public async Task Process_links_to_creation_and_uses_it_as_parent_without_ambient_span()
    {
        using var capture = new Capture();
        var creation = RemoteContext();
        using var process = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Process)
            .Start(new() { CreationContext = creation });
        await Assert.That(process!.ParentSpanId).IsEqualTo(creation.SpanId);
        await Assert.That(process.TraceId).IsEqualTo(creation.TraceId);
        await Assert.That(process.Links.Single().Context).IsEqualTo(creation);
    }

    [Test]
    public async Task Ambient_parent_wins_and_settlement_is_child_of_process()
    {
        using var capture = new Capture();
        using var ambient = new Activity("request").Start();
        var creation = RemoteContext();
        using var process = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Process)
            .Start(new() { CreationContext = creation });
        using var settle = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle, "commit").Start();
        await Assert.That(process!.ParentSpanId).IsEqualTo(ambient.SpanId);
        await Assert.That(process.Links.Single().Context).IsEqualTo(creation);
        await Assert.That(settle!.ParentSpanId).IsEqualTo(process.SpanId);
    }

    [Test]
    public async Task Creation_parent_can_be_disabled()
    {
        using var capture = new Capture(new() { CreationContextAsParent = false });
        var creation = RemoteContext();
        using var process = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Process)
            .Start(new() { CreationContext = creation });
        await Assert.That(process!.ParentSpanId).IsEqualTo(default(ActivitySpanId));
        await Assert.That(process.Links.Single().Context).IsEqualTo(creation);
    }

    [Test]
    [Arguments(MessagingOperation.Send, ActivityKind.Client)]
    [Arguments(MessagingOperation.Process, ActivityKind.Consumer)]
    public async Task Batch_uses_links_and_has_no_single_message_tags(MessagingOperation operation, ActivityKind kind)
    {
        using var capture = new Capture();
        var links = new[] { new ActivityLink(RemoteContext()), new ActivityLink(RemoteContext()) };
        using var activity = new MessageOperationTracing(capture.Tracing, Endpoint, operation).Start(new()
        {
            Links = links, BatchCount = 2, MessageId = "omit", PartitionId = "omit", CreationContext = links[0].Context,
        });
        await Assert.That(activity!.Kind).IsEqualTo(kind);
        await Assert.That(activity.ParentSpanId).IsEqualTo(default(ActivitySpanId));
        await Assert.That(activity.Links.Select(link => link.Context)).IsEquivalentTo(links.Select(link => link.Context));
        await Assert.That(activity.GetTagItem("messaging.batch.message_count")).IsEqualTo(2);
        await Assert.That(activity.GetTagItem("messaging.message.id")).IsNull();
        await Assert.That(activity.GetTagItem("messaging.destination.partition.id")).IsNull();
    }

    [Test]
    public async Task Batch_without_create_spans_uses_send_as_producer()
    {
        using var capture = new Capture(new() { CreateSpansForBatches = false });
        using var send = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start(new() { BatchCount = 2 });
        await Assert.That(capture.Tracing.Options.CreateSpansForBatches).IsFalse();
        await Assert.That(send!.Kind).IsEqualTo(ActivityKind.Producer);
        await Assert.That(send.Links).IsEmpty();
    }

    [Test]
    public async Task Existing_creation_context_skips_create_and_send_links_without_replacing_it()
    {
        using var capture = new Capture();
        var creation = RemoteContext();
        using var create = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Create).Start(new() { CreationContext = creation });
        using var send = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start(new() { CreationContext = creation });
        await Assert.That(create).IsNull();
        await Assert.That(send!.Kind).IsEqualTo(ActivityKind.Client);
        await Assert.That(send.Links.Single().Context).IsEqualTo(creation);
    }

    [Test]
    public async Task Failure_sets_status_and_type_without_exception_events()
    {
        using var capture = new Capture();
        using var activity = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start();
        MessageOperationTracing.Fail(activity, "broker_unavailable");
        await Assert.That(activity!.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(activity.GetTagItem("error.type")).IsEqualTo("broker_unavailable");
        await Assert.That(activity.Events).IsEmpty();
    }

    [Test]
    [Arguments(ActivitySamplingResult.AllDataAndRecorded, 1)]
    [Arguments(ActivitySamplingResult.AllData, 0)]
    [Arguments(ActivitySamplingResult.PropagationData, 0)]
    [Arguments(ActivitySamplingResult.None, 0)]
    public async Task Enrichers_run_only_for_recorded_spans(ActivitySamplingResult sampling, int expected)
    {
        var enricher = new Enricher();
        using var capture = new Capture(sampling: sampling, enrichers: [enricher]);
        using var activity = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start();
        await Assert.That(enricher.Calls).IsEqualTo(expected);
        if (expected != 0) await Assert.That(activity!.DisplayName).IsEqualTo("custom send");
    }

    [Test]
    public async Task No_listener_start_allocates_nothing()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var operation = new MessageOperationTracing(tracing, Endpoint, MessagingOperation.Process);
        var data = new MessageTraceData { CreationContext = RemoteContext() };
        for (var i = 0; i < 1000; i++) operation.Start(data)?.Dispose();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) operation.Start(data)?.Dispose();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Dropped_spans_allocate_nothing_after_warmup(bool batch)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions { IncludeBodySize = true }), []);
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None,
        };
        ActivitySource.AddActivityListener(listener);
        var operation = new MessageOperationTracing(tracing, Endpoint, MessagingOperation.Process);
        // Exclude ActivitySource's root trace ID string allocation from Relayer's sampling preparation.
        using var ambient = new Activity("request").Start();
        var links = new[] { new ActivityLink(RemoteContext()), new ActivityLink(RemoteContext()), new ActivityLink(RemoteContext()) };
        var data = new MessageTraceData
        {
            CreationContext = links[0].Context, PartitionId = "3", MessageId = "id", BodySize = 42,
            BatchCount = batch ? 2 : null, Links = batch ? links.AsMemory(1, 2) : default,
        };
        for (var i = 0; i < 1000; i++) operation.Start(data)?.Dispose();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) operation.Start(data)?.Dispose();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    public async Task Sampling_links_and_partition_survive_reentrant_starts_and_borrowed_buffer_reuse()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var operation = new MessageOperationTracing(tracing, Endpoint, MessagingOperation.Process);
        var first = RemoteContext();
        var second = RemoteContext();
        var nested = false;
        var samples = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> creation) =>
            {
                var expected = nested ? second : first;
                if (!creation.Links!.Single().Context.Equals(expected)) throw new InvalidOperationException("Wrong sampling link.");
                if (!nested)
                {
                    nested = true;
                    using var inner = operation.Start(new() { CreationContext = second, PartitionId = "inner" });
                    nested = false;
                    if (!creation.Links!.Single().Context.Equals(first)) throw new InvalidOperationException("Link overwritten by reentrancy.");
                    if (!Equals(creation.Tags!.Single(tag => tag.Key == "messaging.destination.partition.id").Value, "outer"))
                        throw new InvalidOperationException("Partition overwritten by reentrancy.");
                }
                samples++;
                return ActivitySamplingResult.AllDataAndRecorded;
            },
        };
        ActivitySource.AddActivityListener(listener);
        var borrowed = new[] { new ActivityLink(second), new ActivityLink(first), new ActivityLink(second) };
        using var activity = operation.Start(new() { Links = borrowed.AsMemory(1, 1), PartitionId = "outer" });
        borrowed[1] = new ActivityLink(second);
        using var reused = operation.Start(new() { CreationContext = first, PartitionId = "outer" });
        await Assert.That(samples).IsEqualTo(4);
        await Assert.That(activity!.Links.Single().Context).IsEqualTo(first);
        await Assert.That(activity.GetTagItem("messaging.destination.partition.id")).IsEqualTo("outer");
    }

    [Test]
    public async Task Concurrent_starts_do_not_share_sampling_state()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var operation = new MessageOperationTracing(tracing, Endpoint, MessagingOperation.Process);
        using var barrier = new Barrier(8);
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> creation) =>
            {
                barrier.SignalAndWait();
                var partition = creation.Tags!.Single(tag => tag.Key == "messaging.destination.partition.id").Value;
                if (!Equals(partition, creation.Links!.Single().Context.SpanId.ToHexString()))
                    throw new InvalidOperationException("Concurrent sampling state overwritten.");
                return ActivitySamplingResult.AllDataAndRecorded;
            },
        };
        ActivitySource.AddActivityListener(listener);
        var tasks = new Task[8];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = Task.Factory.StartNew(() =>
            {
                var context = RemoteContext();
                using var activity = operation.Start(new() { CreationContext = context, PartitionId = context.SpanId.ToHexString() });
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await Task.WhenAll(tasks);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Context_parser_matches_framework_for_valid_and_invalid_inputs(bool utf8)
    {
        const string valid = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        string[] inputs = [valid, "01" + valid[2..], "fe" + valid[2..], "ff" + valid[2..],
            valid[..53] + "ff", valid[..53] + "00", valid.ToUpperInvariant(), valid + "-extra", valid[..54], "",
            valid[..3] + new string('0', 32) + valid[35..], valid[..36] + new string('0', 16) + valid[52..],
            valid[..10] + "g" + valid[11..], valid[..53] + "0G", valid.Replace('-', '_'), valid.Replace('-', 'é')];
        var names = new HeaderNames();
        foreach (var input in inputs)
        {
            var headers = new Dictionary<string, WireString>
            {
                [names.TraceParent] = utf8 ? new(Encoding.UTF8.GetBytes(input)) : new(input),
                [names.TraceState] = new("vendor=value"),
            };
            ActivityContext.TryParse(input, "vendor=value", true, out var expected);
            await Assert.That(MessageCreationContext.Read<Dictionary<string, WireString>, Binding>(headers, names)).IsEqualTo(expected);
        }
    }

    [Test]
    public async Task Utf8_context_parsing_allocates_only_framework_context_storage()
    {
        const string parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        var names = new HeaderNames();
        var utf8 = new Dictionary<string, WireString> { [names.TraceParent] = new(Encoding.UTF8.GetBytes(parent)) };
        var text = new Dictionary<string, WireString> { [names.TraceParent] = new(parent) };
        for (var i = 0; i < 1000; i++)
        {
            MessageCreationContext.Read<Dictionary<string, WireString>, Binding>(utf8, names);
            MessageCreationContext.Read<Dictionary<string, WireString>, Binding>(text, names);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) MessageCreationContext.Read<Dictionary<string, WireString>, Binding>(text, names);
        var textBytes = GC.GetAllocatedBytesForCurrentThread() - before;
        before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) MessageCreationContext.Read<Dictionary<string, WireString>, Binding>(utf8, names);
        var utf8Bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(utf8Bytes).IsEqualTo(textBytes);
    }

    [Test]
    public async Task Temporary_names_omit_destination_and_template_takes_precedence()
    {
        using var capture = new Capture();
        using (var temporary = new MessageOperationTracing(capture.Tracing,
            new() { System = "servicebus", Destination = "random", Temporary = true }, MessagingOperation.Send).Start())
            await Assert.That(temporary!.DisplayName).IsEqualTo("send");
        using var templated = new MessageOperationTracing(capture.Tracing,
            new() { System = "servicebus", Destination = "reply-123", Anonymous = true, DestinationTemplate = "reply-{id}" }, MessagingOperation.Send).Start();
        await Assert.That(templated!.DisplayName).IsEqualTo("send reply-{id}");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Body_size_is_opt_in(bool enabled)
    {
        using var capture = new Capture(new() { IncludeBodySize = enabled });
        using var activity = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start(new() { BodySize = 42 });
        await Assert.That(activity!.GetTagItem("messaging.message.body.size")).IsEqualTo(enabled ? (object)42L : null);
    }

    [Test]
    public async Task External_utf8_context_survives_repeated_writes()
    {
        var headers = new Dictionary<string, WireString>();
        var names = new HeaderNames("custom:");
        var original = RemoteContext();
        await Assert.That(MessageCreationContext.WriteIfAbsent<Dictionary<string, WireString>, Binding>(headers, names, original)).IsTrue();
        headers[names.TraceParent] = new(Encoding.UTF8.GetBytes(headers[names.TraceParent].ToString()));
        var before = headers[names.TraceParent].Utf8;
        await Assert.That(MessageCreationContext.WriteIfAbsent<Dictionary<string, WireString>, Binding>(headers, names, RemoteContext())).IsFalse();
        var read = MessageCreationContext.Read<Dictionary<string, WireString>, Binding>(headers, names);
        await Assert.That(read.TraceId).IsEqualTo(original.TraceId);
        await Assert.That(read.SpanId).IsEqualTo(original.SpanId);
        await Assert.That(read.TraceState).IsEqualTo("vendor=value");
        await Assert.That(read.IsRemote).IsTrue();
        await Assert.That(headers[names.TraceParent].Utf8).IsEqualTo(before);
        headers[names.TraceParent] = new("invalid");
        await Assert.That(MessageCreationContext.WriteIfAbsent<Dictionary<string, WireString>, Binding>(headers, names, original)).IsFalse();
        await Assert.That(MessageCreationContext.Read<Dictionary<string, WireString>, Binding>(headers, names)).IsEqualTo(default(ActivityContext));
    }

    private static ActivityContext RemoteContext() => new(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded, "vendor=value", true);

    [Test]
    public async Task Batch_send_links_to_each_create_span_and_consumer_links_to_the_same_context()
    {
        using var capture = new Capture();
        var create = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Create);
        ActivityContext first;
        ActivityContext second;
        using (var activity = create.Start()) first = activity!.Context;
        using (var activity = create.Start()) second = activity!.Context;
        using (var send = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start(new()
        {
            BatchCount = 2, Links = new[] { new ActivityLink(first), new ActivityLink(second) },
        }))
        {
            await Assert.That(send!.Kind).IsEqualTo(ActivityKind.Client);
            await Assert.That(send.Links.Select(link => link.Context)).IsEquivalentTo(new[] { first, second });
        }
        using var process = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Process).Start(new() { CreationContext = first });
        await Assert.That(process!.ParentSpanId).IsEqualTo(first.SpanId);
        await Assert.That(process.Links.Single().Context).IsEqualTo(first);
    }

    [Test]
    public async Task Invalid_operation_metadata_fails_at_composition()
    {
        using var capture = new Capture();
        await Assert.That(() => new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Settle))
            .Throws<ArgumentException>();
        await Assert.That(() => new MessageOperationTracing(capture.Tracing, Endpoint, (MessagingOperation)99))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Process, "receive"))
            .Throws<ArgumentException>();
    }

    [Test]
    [Arguments(false, 0)]
    [Arguments(false, -1)]
    [Arguments(true, 0)]
    [Arguments(true, -1)]
    public async Task Invalid_batch_count_fails_with_or_without_listener(bool listener, int count)
    {
        using var capture = listener ? new Capture() : null;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var operation = new MessageOperationTracing(capture?.Tracing ?? tracing, Endpoint, MessagingOperation.Send);
        await Assert.That(() => operation.Start(new() { BatchCount = count })).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Create_rejects_batch_even_with_existing_context_or_no_listener(bool listener, bool existingContext)
    {
        using var capture = listener ? new Capture() : null;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var operation = new MessageOperationTracing(capture?.Tracing ?? tracing, Endpoint, MessagingOperation.Create);
        var data = new MessageTraceData { BatchCount = 1, CreationContext = existingContext ? RemoteContext() : default };
        await Assert.That(() => operation.Start(data)).Throws<ArgumentException>();
    }

    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments(" ")]
    public async Task Failure_with_missing_error_type_uses_fallback(string? errorType)
    {
        using var capture = new Capture();
        using var activity = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start();
        MessageOperationTracing.Fail(activity, errorType!);
        await Assert.That(activity!.Status).IsEqualTo(ActivityStatusCode.Error);
        await Assert.That(activity.GetTagItem("error.type")).IsEqualTo("_OTHER");
        await Assert.That(activity.Events).IsEmpty();
    }

    [Test]
    public async Task Throwing_enricher_does_not_interrupt_operation_or_leak_current_activity()
    {
        var enricher = new Enricher();
        using var capture = new Capture(enrichers: [new ThrowingEnricher(), enricher]);
        using var ambient = new Activity("request").Start();
        using (var activity = new MessageOperationTracing(capture.Tracing, Endpoint, MessagingOperation.Send).Start())
        {
            await Assert.That(activity).IsNotNull();
            await Assert.That(Activity.Current).IsSameReferenceAs(activity);
        }
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
        await Assert.That(enricher.Calls).IsEqualTo(1);
    }

    private sealed class ThrowingEnricher : IActivityEnricher
    {
        public void Enrich(Activity activity) => throw new InvalidOperationException("Enricher failed.");
    }

    private sealed class Binding : IMessageHeaderReader<Dictionary<string, WireString>>, IMessageHeaderWriter<Dictionary<string, WireString>>
    {
        public static bool TryGetHeader(Dictionary<string, WireString> message, string name, out WireString value) => message.TryGetValue(name, out value);
        public static void SetHeader(Dictionary<string, WireString> message, string name, ReadOnlySpan<char> value) => message[name] = new(value.ToString());
        public static void RemoveHeader(Dictionary<string, WireString> message, string name) => message.Remove(name);
    }

    private sealed class Enricher : IActivityEnricher
    {
        internal int Calls;
        public void Enrich(Activity activity) { Calls++; activity.DisplayName = "custom send"; }
    }

    private sealed class Capture : IDisposable
    {
        internal MessageTracing Tracing { get; }
        internal Dictionary<string, object?> SampleTags { get; } = [];
        private readonly ActivityListener _listener;
        internal Capture(TracingOptions? options = null, ActivitySamplingResult sampling = ActivitySamplingResult.AllDataAndRecorded, IActivityEnricher[]? enrichers = null)
        {
            Tracing = new(Options.Create(options ?? new()), enrichers ?? []);
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, Tracing.Source),
                Sample = (ref ActivityCreationOptions<ActivityContext> creation) =>
                {
                    SampleTags.Clear();
                    foreach (var tag in creation.Tags!) SampleTags[tag.Key] = tag.Value;
                    return sampling;
                },
            };
            ActivitySource.AddActivityListener(_listener);
        }
        public void Dispose() { _listener.Dispose(); Tracing.Dispose(); }
    }
}
