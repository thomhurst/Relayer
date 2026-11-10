using System.Diagnostics;
using System.Text;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public class SqsSendTests
{
    [Test]
    public async Task Hosted_sender_serializes_the_real_generated_envelope()
    {
        using var client = new SqsTestClient();
        var app = Host.CreateApplicationBuilder();
        app.Services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AmazonSqs("test", sqs =>
            {
                sqs.Options.Configure(o => { o.SqsClient = client; o.Queues.Add(new("orders")); });
                sqs.Send<Order>("orders");
            });
        });
        using var host = app.Build();
        await host.StartAsync();
        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new Order());
            var sent = client.Sent.Single();
            await Assert.That(sent.Body).IsEqualTo("{}");
            await Assert.That(sent.Attributes["ce_type"]).Contains("sqs.test.order");
            await Assert.That(sent.Attributes["ce_specversion"]).IsEqualTo("1.0");
            await Assert.That(sent.Attributes["content-type"]).IsEqualTo("application/json");
            await Assert.That(sent.Attributes.Count <= 10).IsTrue();
        }
        finally { await host.StopAsync(); }
    }

    [Test]
    public async Task Custom_fifo_identity_binding_preserves_distinct_events_and_replays()
    {
        using var client = new SqsTestClient();
        var app = Host.CreateApplicationBuilder();
        app.Services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AmazonSqs("test", sqs =>
            {
                sqs.Options.Configure(o =>
                {
                    o.SqsClient = client;
                    o.Queues.Add(new("ordered.fifo") { Attributes = new() { ["FifoQueue"] = "true" } });
                });
                sqs.Send<Order>("ordered.fifo", route =>
                {
                    route.SendOptions = new() { HeaderNames = new("event_") };
                    route.OrderingKey(_ => "customer");
                });
            });
        });
        using var host = app.Build();
        await host.StartAsync();
        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendNowAsync(new Order());
            await sender.SendNowAsync(new Order());
            var first = client.Sent[0];
            var second = client.Sent[1];
            await Assert.That(first.Body).IsEqualTo(second.Body);
            await Assert.That(first.Deduplication).IsEqualTo(first.Attributes["event_id"]);
            await Assert.That(second.Deduplication).IsEqualTo(second.Attributes["event_id"]);
            await Assert.That(first.Deduplication).IsNotEqualTo(second.Deduplication);
            var transport = host.Services.GetServices<ITransport>().Single();
            var replay = new OutgoingMessage("ordered.fifo", Encoding.UTF8.GetBytes(first.Body),
                first.Attributes.Select(pair => new KeyValuePair<string, WireString>(pair.Key, new(pair.Value))).ToArray(),
                new WireString("customer"));
            await transport.SendAsync(replay, default);
            await transport.SendBatchAsync(new[] { replay, replay }, default);
            await Assert.That(client.Sent[2].Deduplication).IsEqualTo(first.Deduplication);
            await Assert.That(client.Batches.Single().All(entry => entry.Message.Deduplication == first.Deduplication)).IsTrue();
        }
        finally { await host.StopAsync(); }
    }

    [Test]
    public async Task Conflicting_fifo_identity_bindings_fail_before_native_calls()
    {
        using var client = new SqsTestClient();
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.AmazonSqs("test", sqs =>
        {
            sqs.Options.Configure(o =>
            {
                o.SqsClient = client;
                o.Queues.Add(new("ordered.fifo") { Attributes = new() { ["FifoQueue"] = "true" } });
            });
            sqs.Send<Order>("ordered.fifo");
            sqs.Send<Audit>("ordered.fifo", route => route.SendOptions = new() { HeaderNames = new("event_") });
        }));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray()).Throws<InvalidOperationException>()
            .WithMessage("SQS FIFO queue 'ordered.fifo': CloudEvents routes must use the same HeaderNames identity binding. Configure matching HeaderNames for every route to this queue.");
        await Assert.That(client.Lookups).IsEqualTo(0);
        await Assert.That(client.Sent.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Named_registration_preserves_options_and_caller_owned_clients()
    {
        using var first = new SqsTestClient();
        using var second = new SqsTestClient();
        var config = new AmazonSQSConfig { ServiceURL = "http://localhost:4566" };
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.AmazonSqs("first", sqs =>
            {
                sqs.Options.Configure(o => { o.SqsClient = first; o.Sqs = config; o.Queues.Add(new("orders")); });
                sqs.Send<Order>("orders");
            });
            builder.AmazonSqs("second", sqs =>
            {
                sqs.Options.Configure(o => { o.SqsClient = second; o.Queues.Add(new("audit")); });
                sqs.Send<Audit>("audit");
            });
        });
        await using (var provider = services.BuildServiceProvider())
        {
            var options = provider.GetRequiredService<IOptionsMonitor<AmazonSqsTransportOptions>>();
            await Assert.That(options.Get("first").Sqs).IsSameReferenceAs(config);
            var transports = provider.GetServices<ITransport>().ToArray();
            foreach (var transport in transports) await transport.StartAsync(default);
            await transports[0].SendAsync(new("orders", "{}"u8.ToArray()), default);
            await transports[1].SendAsync(new("audit", "{}"u8.ToArray()), default);
            await Assert.That(first.Sent.Single().Url).Contains("orders");
            await Assert.That(second.Sent.Single().Url).Contains("audit");
        }
        await Assert.That(first.Disposals).IsEqualTo(0);
        await Assert.That(second.Disposals).IsEqualTo(0);
    }

    [Test]
    public async Task Headers_callbacks_fifo_identity_and_state_are_preserved()
    {
        using var client = new SqsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var calls = 0;
        var options = new SendOptions().AmazonSqs(request =>
        {
            calls++;
            request.MessageAttributes["native"] = new() { DataType = "String", StringValue = "value" };
            request.MessageAttributes["traceparent"] = new() { DataType = "String", StringValue = "wrong" };
            request.MessageDeduplicationId = "wrong";
        });
        KeyValuePair<string, WireString>[] headers = [new("ce_id", new("event-1")), new("traceparent", new("creation"u8.ToArray()))];
        await transport.SendAsync(new("ordered.fifo", "héllo"u8.ToArray(), headers, new WireString("customer"), options: options), default);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        var sent = client.Sent;
        await Assert.That(calls).IsEqualTo(1);
        await Assert.That(sent[0].Body).IsEqualTo("héllo");
        await Assert.That(sent[0].Group).IsEqualTo("customer");
        await Assert.That(sent[0].Deduplication).IsEqualTo("event-1");
        await Assert.That(sent[0].Attributes["traceparent"]).IsEqualTo("creation");
        await Assert.That(sent[0].Attributes["native"]).IsEqualTo("value");
        await Assert.That(sent[1].Group).IsNull();
        await Assert.That(sent[1].Deduplication).IsNull();
        await Assert.That(sent[1].Attributes.Count).IsEqualTo(0);
        await Assert.That(client.Requests[0]).IsSameReferenceAs(client.Requests[1]);
    }

    [Test]
    public async Task Utf8_fifo_group_reuses_the_string_seen_by_the_native_callback()
    {
        var state = new SqsSendState("0");
        string? decodedGroup = null;
        var options = new SendOptions().AmazonSqs(request =>
        {
            decodedGroup = request.MessageGroupId;
            request.MessageGroupId = "callback-override";
        });
        var message = new OutgoingMessage("ordered.fifo", "{}"u8.ToArray(),
            orderingKey: new WireString("customer-123"u8.ToArray()), options: options);

        state.Prepare(message, "https://sqs.example/ordered.fifo", true);
        state.CopyToEntry();

        await Assert.That(decodedGroup).IsEqualTo("customer-123");
        await Assert.That(state.Request.MessageGroupId).IsSameReferenceAs(decodedGroup);
        await Assert.That(state.Entry.MessageGroupId).IsSameReferenceAs(decodedGroup);
    }

    [Test]
    public async Task Native_replacement_dictionaries_remain_caller_owned()
    {
        using var client = new SqsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var attributes = new Dictionary<string, MessageAttributeValue> { ["native"] = new() { DataType = "String", StringValue = "keep" } };
        var options = new SendOptions().AmazonSqs(request => request.MessageAttributes = attributes);
        await transport.SendAsync(new("orders", "{}"u8.ToArray(), options: options), default);
        await Assert.That(attributes["native"].StringValue).IsEqualTo("keep");
        await Assert.That(attributes.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Batches_split_by_count_bytes_and_destination_without_repeating_callbacks()
    {
        using var client = new SqsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var calls = 0;
        var options = new SendOptions().AmazonSqs(_ => calls++);
        var messages = new List<OutgoingMessage>();
        for (var i = 0; i < 11; i++) messages.Add(new("orders", "{}"u8.ToArray(), options: options));
        var large = Encoding.UTF8.GetBytes(new string('x', 600_000));
        messages.Add(new("orders", large, options: options));
        messages.Add(new("orders", large, options: options));
        messages.Add(new("ordered.fifo", "{}"u8.ToArray(), orderingKey: new WireString("key"), options:
            new SendOptions().AmazonSqs(request => { calls++; request.MessageDeduplicationId = "native-id"; })));
        await transport.SendBatchAsync(messages.ToArray(), default);
        await Assert.That(client.Batches.Select(batch => batch.Length).SequenceEqual(new[] { 10, 2, 1, 1 })).IsTrue();
        await Assert.That(calls).IsEqualTo(messages.Count);
        await Assert.That(client.Batches.All(batch => batch.Select(item => item.Id).SequenceEqual(Enumerable.Range(0, batch.Length).Select(i => i.ToString())))).IsTrue();
        await Assert.That(client.Batches[^1][0].Message.Group).IsEqualTo("key");
    }

    [Test]
    public async Task Partial_batch_failure_exposes_native_response_and_does_not_retry_successes()
    {
        using var client = new SqsTestClient { FailBatch = true };
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        AmazonSqsBatchException? failure = null;
        try { await transport.SendBatchAsync(new OutgoingMessage[] { new("orders", "{}"u8.ToArray()), new("orders", "{}"u8.ToArray()) }, default); }
        catch (AmazonSqsBatchException error) { failure = error; }
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Response.Failed.Single().Id).IsEqualTo("1");
        await Assert.That(failure.Response.Successful.Single().Id).IsEqualTo("0");
        await Assert.That(failure.BatchOffset).IsEqualTo(0);
        await Assert.That(client.Batches.Count).IsEqualTo(1);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        await Assert.That(client.Sent.Single().Attributes.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Timers_use_native_seconds_and_do_not_leak_to_following_messages()
    {
        using var client = new SqsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        await transport.SendAsync(new("orders", "{}"u8.ToArray(), deliverAt: DateTimeOffset.UtcNow.AddMinutes(10)), default);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        await Assert.That(client.Sent[0].Delay is > 590 and <= 600).IsTrue();
        await Assert.That(client.Sent[1].Delay).IsNull();
        await transport.SendAsync(new("orders", "{}"u8.ToArray(), deliverAt: DateTimeOffset.UtcNow.AddMinutes(-1),
            options: new SendOptions().AmazonSqs(request => request.DelaySeconds = 900)), default);
        await Assert.That(client.Sent[2].Delay).IsEqualTo(0);
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), deliverAt: DateTimeOffset.UtcNow.AddHours(1)), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendAsync(new("ordered.fifo", "{}"u8.ToArray(), orderingKey: new WireString("key"), deliverAt: DateTimeOffset.UtcNow.AddMinutes(1)), default)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Invalid_attributes_payload_ordering_and_size_fail_without_native_sends()
    {
        using var client = new SqsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var headers = Enumerable.Range(0, 11).Select(i => new KeyValuePair<string, WireString>("h" + i, new("value"))).ToArray();
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), headers), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[] { 0xff }), default)).Throws<DecoderFallbackException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), orderingKey: new WireString("key")), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendAsync(new("ordered.fifo", "{}"u8.ToArray()), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", Encoding.UTF8.GetBytes(new string('x', 1_048_577))), default)).Throws<InvalidOperationException>();
        await Assert.That(client.Sent.Count).IsEqualTo(0);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        await Assert.That(client.Sent.Single().Attributes.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Concurrent_sends_have_independent_borrowed_storage()
    {
        using var client = new SqsTestClient { Gate = new(TaskCreationOptions.RunContinuationsAsynchronously) };
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var first = transport.SendAsync(new("orders", "first"u8.ToArray()), default);
        var second = transport.SendAsync(new("orders", "second"u8.ToArray()), default);
        await Assert.That(client.Requests.Count).IsEqualTo(2);
        await Assert.That(ReferenceEquals(client.Requests[0], client.Requests[1])).IsFalse();
        await Assert.That(client.Requests[0].MessageBody).IsEqualTo("first");
        await Assert.That(client.Requests[1].MessageBody).IsEqualTo("second");
        client.Gate.SetResult();
        await first;
        await second;
    }

    [Test]
    public async Task Topology_validate_is_read_only_and_provision_never_updates_existing_queues()
    {
        using var client = new SqsTestClient { Missing = true };
        var options = Options(client);
        options.Queues[0].Attributes = new() { ["MaximumMessageSize"] = "4096" };
        await using var transport = new SqsSendTransport("test", client, false, options);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count).IsEqualTo(0);
        await Assert.That(client.Lookups).IsEqualTo(0);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, default)).Differences.Count).IsEqualTo(2);
        await Assert.That(client.Creates.Count).IsEqualTo(0);
        var provision = await transport.ApplyTopologyAsync(TopologyMode.Provision, default);
        await Assert.That(provision.Differences.Count).IsEqualTo(1);
        await Assert.That(client.Creates.Count).IsEqualTo(1);
        await Assert.That(client.Creates[0]).IsSameReferenceAs(options.Queues[0]);
        await Assert.That(transport.GetCapabilities("orders").MaxMessageBytes).IsEqualTo(1_048_576);
        await Assert.That(transport.GetCapabilities("ordered.fifo").OrderedByKey).IsTrue();
        await transport.ApplyTopologyAsync(TopologyMode.Provision, default);
        await Assert.That(client.Creates.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Native_policy_json_compares_structurally_and_reports_real_drift()
    {
        using var client = new SqsTestClient();
        var options = Options(client);
        options.Queues[0].Attributes = new() { ["Policy"] = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" };
        client.NativeAttributes["Policy"] = "{ \"Statement\": [], \"Version\": \"2012-10-17\" }";
        await using var transport = new SqsSendTransport("test", client, false, options);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, default)).Differences.Count).IsEqualTo(0);
        options.Queues[0].Attributes["Policy"] = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\"}]}";
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, default)).Differences.Single()).Contains("Policy");
        await Assert.That(client.Creates.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Declarations_and_unsupported_receivers_fail_early()
    {
        var options = new AmazonSqsTransportOptions();
        options.Queues.Add(new("wrong.fifo"));
        await Assert.That(() => SqsSendTransport.ValidateDeclarations("test", options, [])).Throws<InvalidOperationException>();
        options.Queues.Clear();
        await Assert.That(() => SqsSendTransport.ValidateDeclarations("test", options, ["missing"])).Throws<InvalidOperationException>();
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.AmazonSqs("test", sqs => sqs.Consume("orders", consumer => consumer.Handler<OrderHandler>())));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray()).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Cancellation_disposal_and_failed_callbacks_return_clean_state()
    {
        var client = new SqsTestClient();
        var transport = new SqsSendTransport("test", client, true, Options(client));
        await transport.StartAsync(default);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray()), cancelled.Token)).Throws<OperationCanceledException>();
        var options = new SendOptions().AmazonSqs(request => { request.MessageGroupId = "bad"; throw new InvalidOperationException("callback"); });
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), options: options), default)).Throws<InvalidOperationException>();
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        await Assert.That(client.Sent.Single().Group).IsNull();
        await transport.DisposeAsync();
        await transport.DisposeAsync();
        await Assert.That(client.Disposals).IsEqualTo(1);
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray()), default)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Send_telemetry_uses_sqs_system_queue_url_and_native_request_id()
    {
        using var client = new SqsTestClient();
        var services = new ServiceCollection();
        services.AddRelayer(_ => { });
        await using var provider = services.BuildServiceProvider();
        var tracing = provider.GetRequiredService<MessageTracing>();
        var spans = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(listener);
        await using var transport = new SqsSendTransport("test", client, false, Options(client), tracing);
        await transport.StartAsync(default);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        var span = spans.Single();
        await Assert.That(span.GetTagItem("messaging.system")).IsEqualTo("aws_sqs");
        await Assert.That(span.GetTagItem("aws.sqs.queue.url")).IsEqualTo("http://localhost/queues/orders");
        await Assert.That(span.GetTagItem("aws.request_id")).IsEqualTo("request-1");
        await Assert.That(client.Sent.Single().Attributes.ContainsKey("traceparent")).IsTrue();
        client.FailBatch = true;
        await Assert.That(async () => await transport.SendBatchAsync(new OutgoingMessage[] { new("orders", "{}"u8.ToArray()) }, default)).Throws<AmazonSqsBatchException>();
        var failed = spans.Last();
        await Assert.That(failed.GetTagItem("aws.request_id")).IsEqualTo("batch-request");
        await Assert.That(failed.GetTagItem("aws.sqs.queue.url")).IsEqualTo("http://localhost/queues/orders");
        await Assert.That(failed.Status).IsEqualTo(ActivityStatusCode.Error);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Native_metrics_wait_for_completion_and_include_failures(bool batch, bool fail)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var durations = new SendDurationCapture(instrumentation);
        long sent = 0;
        using var meter = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (instrument, observer) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.sent.messages")
                    observer.EnableMeasurementEvents(instrument);
            },
        };
        meter.SetMeasurementEventCallback<long>((_, value, _, _) => sent += value);
        meter.Start();
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var client = new SqsTestClient { Gate = new(TaskCreationOptions.RunContinuationsAsynchronously) };
        await using var transport = new SqsSendTransport("test", client, false, Options(client), tracing);
        await transport.StartAsync(default);
        var message = new OutgoingMessage("orders", "{}"u8.ToArray());
        var pending = batch ? transport.SendBatchAsync(new[] { message, message }, default) : transport.SendAsync(message, default);
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(sent).IsEqualTo(batch ? 2L : 1L);
        await Assert.That(durations.Values.Count).IsEqualTo(0);
        if (fail) client.Gate.SetException(new AmazonSQSException("failure"));
        else client.Gate.SetResult();
        if (fail) await Assert.That(async () => await pending).Throws<AmazonSQSException>();
        else await pending;
        var duration = durations.Values.Single();
        await Assert.That(duration.Tags["messaging.system"]).IsEqualTo("aws_sqs");
        await Assert.That(duration.Tags["messaging.destination.name"]).IsEqualTo("orders");
        await Assert.That(duration.Tags.ContainsKey("error.type")).IsEqualTo(fail);
    }

    [Test]
    public async Task Split_batches_count_native_attempts_and_partial_failure_stops_later_batches()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var durations = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var client = new SqsTestClient();
        await using var transport = new SqsSendTransport("test", client, false, Options(client), tracing);
        await transport.StartAsync(default);
        var messages = Enumerable.Repeat(new OutgoingMessage("orders", "{}"u8.ToArray()), 11).ToArray();
        await transport.SendBatchAsync(messages, default);
        await Assert.That(durations.Values.Count).IsEqualTo(2);
        durations.Values.Clear();
        client.FailBatch = true;
        await Assert.That(async () => await transport.SendBatchAsync(messages, default)).Throws<AmazonSqsBatchException>();
        await Assert.That(durations.Values.Single().Tags["error.type"]).IsEqualTo("_OTHER");
        durations.Values.Clear();
        await transport.SendBatchAsync(Array.Empty<OutgoingMessage>(), default);
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[] { 0xff }), default)).Throws<DecoderFallbackException>();
        await Assert.That(durations.Values.Count).IsEqualTo(0);
    }

    internal static AmazonSqsTransportOptions Options(IAmazonSQS client)
    {
        var options = new AmazonSqsTransportOptions { SqsClient = client };
        options.Queues.Add(new("orders"));
        options.Queues.Add(new("ordered.fifo") { Attributes = new() { ["FifoQueue"] = "true", ["ContentBasedDeduplication"] = "true" } });
        return options;
    }

    private static SqsSendTransport Transport(IAmazonSQS client) => new("test", client, false, Options(client));
    [Message("sqs.test.order")]
    public sealed record Order;
    [Message("sqs.test.audit")]
    public sealed record Audit;
    public sealed class OrderHandler : IHandler<Order>
    {
        public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}

internal sealed record SqsSent(string Url, string Body, string? Group, string? Deduplication, int? Delay, Dictionary<string, string> Attributes);

internal sealed class SqsTestClient() : AmazonSQSClient(new AnonymousAWSCredentials(), new AmazonSQSConfig { ServiceURL = "http://localhost:4566" })
{
    internal readonly List<SqsSent> Sent = [];
    internal readonly List<SendMessageRequest> Requests = [];
    internal readonly List<(string Id, SqsSent Message)[]> Batches = [];
    internal readonly List<CreateQueueRequest> Creates = [];
    internal readonly Dictionary<string, string> NativeAttributes = new() { ["MaximumMessageSize"] = "1048576", ["FifoQueue"] = "true", ["ContentBasedDeduplication"] = "true" };
    internal bool FailBatch;
    internal bool Missing;
    internal int Lookups;
    internal int Disposals;
    internal TaskCompletionSource? Gate;

    public override async Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
    {
        Requests.Add(request);
        Sent.Add(Snapshot(request));
        if (Gate is not null) await Gate.Task.WaitAsync(cancellationToken);
        return new() { ResponseMetadata = new() { RequestId = "request-1" } };
    }

    public override async Task<SendMessageBatchResponse> SendMessageBatchAsync(SendMessageBatchRequest request, CancellationToken cancellationToken = default)
    {
        Batches.Add(request.Entries.Select(entry => (entry.Id, Snapshot(new()
        {
            QueueUrl = request.QueueUrl, MessageBody = entry.MessageBody, MessageGroupId = entry.MessageGroupId,
            MessageDeduplicationId = entry.MessageDeduplicationId, DelaySeconds = entry.DelaySeconds, MessageAttributes = entry.MessageAttributes,
        }))).ToArray());
        if (Gate is not null) await Gate.Task.WaitAsync(cancellationToken);
        return new SendMessageBatchResponse
        {
            Successful = [new() { Id = "0" }],
            Failed = FailBatch ? [new() { Id = "1", Code = "InvalidMessageContents", SenderFault = true }] : [],
            ResponseMetadata = new() { RequestId = "batch-request" },
        };
    }

    public override Task<GetQueueUrlResponse> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default)
    {
        Lookups++;
        if (Missing) throw new QueueDoesNotExistException("missing");
        return Task.FromResult(new GetQueueUrlResponse { QueueUrl = "http://localhost/queues/" + queueName });
    }
    public override Task<CreateQueueResponse> CreateQueueAsync(CreateQueueRequest request, CancellationToken cancellationToken = default)
    {
        Creates.Add(request);
        Missing = false;
        return Task.FromResult(new CreateQueueResponse { QueueUrl = "http://localhost/queues/" + request.QueueName });
    }
    public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request, CancellationToken cancellationToken = default) =>
        Task.FromResult(new GetQueueAttributesResponse { Attributes = NativeAttributes });
    protected override void Dispose(bool disposing) { if (disposing) Disposals++; base.Dispose(disposing); }

    private static SqsSent Snapshot(SendMessageRequest request) => new(request.QueueUrl, request.MessageBody, request.MessageGroupId,
        request.MessageDeduplicationId, request.DelaySeconds,
        request.MessageAttributes?.ToDictionary(item => item.Key, item => item.Value.StringValue) ?? []);
}
