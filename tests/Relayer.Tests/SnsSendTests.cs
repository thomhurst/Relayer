using System.Diagnostics;
using System.Text;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public class SnsSendTests
{
    [Test]
    public async Task Hosted_sender_uses_native_topics_and_generated_envelope_without_owning_client()
    {
        using var client = new SnsTestClient();
        var app = Host.CreateApplicationBuilder();
        app.Services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.AmazonSns("sns", sns =>
            {
                sns.Options.Configure(o => { o.SnsClient = client; o.Topics.Add(new("orders")); });
                sns.Send<Event>("orders");
            });
        });
        using (var host = app.Build())
        {
            await host.StartAsync();
            try
            {
                await using var scope = host.Services.CreateAsyncScope();
                await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new Event());
                var sent = client.Sent.Single();
                await Assert.That(sent.Arn).IsEqualTo(SnsTestClient.Arn("orders"));
                await Assert.That(sent.Body).IsEqualTo("{}");
                await Assert.That(sent.Attributes["ce_type"]).Contains("sns.test.event");
                await Assert.That(sent.Attributes["ce_specversion"]).IsEqualTo("1.0");
            }
            finally { await host.StopAsync(); }
        }
        await Assert.That(client.Disposals).IsEqualTo(0);
    }

    [Test]
    public async Task Fifo_headers_native_callbacks_and_replacement_dictionaries_preserve_ownership_and_pool_reset()
    {
        using var client = new SnsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var attributes = new Dictionary<string, MessageAttributeValue> { ["native"] = new() { DataType = "String", StringValue = "keep" } };
        var calls = 0;
        var options = new SendOptions().AmazonSns(request =>
        {
            calls++;
            request.TopicArn = "wrong";
            request.MessageGroupId = "wrong";
            request.MessageDeduplicationId = "wrong";
            request.Subject = "subject";
            request.MessageAttributes = attributes;
        });
        KeyValuePair<string, WireString>[] headers = [new("ce_id", new("event-1")), new("traceparent", new("creation"u8.ToArray()))];
        var message = new OutgoingMessage("ordered.fifo", "héllo"u8.ToArray(), headers, new WireString("customer"u8.ToArray()), options: options);
        await transport.SendAsync(message, default);
        await transport.SendBatchAsync(new[] { message }, default);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        await Assert.That(calls).IsEqualTo(2);
        var sent = client.Sent[0];
        await Assert.That(sent.Arn).IsEqualTo(SnsTestClient.Arn("ordered.fifo"));
        await Assert.That(sent.Body).IsEqualTo("héllo");
        await Assert.That(sent.Group).IsEqualTo("customer");
        await Assert.That(sent.Deduplication).IsEqualTo("event-1");
        await Assert.That(sent.Attributes["traceparent"]).IsEqualTo("creation");
        await Assert.That(client.Batches.Single()[0].Message.Subject).IsEqualTo("subject");
        await Assert.That(client.Sent[1].Subject).IsNull();
        await Assert.That(client.Sent[1].Group).IsNull();
        await Assert.That(client.Sent[1].Deduplication).IsNull();
        await Assert.That(client.Sent[1].Attributes.Count).IsEqualTo(0);
        await Assert.That(attributes.Count).IsEqualTo(1);
        await Assert.That(attributes["native"].StringValue).IsEqualTo("keep");
        await Assert.That(client.Requests[0]).IsSameReferenceAs(client.Requests[1]);
    }

    [Test]
    public async Task Custom_fifo_identity_replays_and_native_deduplication_are_supported()
    {
        using var client = new SnsTestClient();
        await using var transport = new SnsSendTransport("sns", client, false, Options(client), identityHeaders: new Dictionary<string, string> { ["ordered.fifo"] = "event_id" });
        await transport.StartAsync(default);
        var message = new OutgoingMessage("ordered.fifo", "{}"u8.ToArray(), new KeyValuePair<string, WireString>[] { new("event_id", new("stable")) }, new WireString("group"));
        await transport.SendAsync(message, default);
        await transport.SendBatchAsync(new[] { message, message }, default);
        await Assert.That(client.Sent.Single().Deduplication).IsEqualTo("stable");
        await Assert.That(client.Batches.Single().All(entry => entry.Message.Deduplication == "stable")).IsTrue();
        await transport.SendAsync(new("ordered.fifo", "{}"u8.ToArray(), orderingKey: new WireString("group"), options:
            new SendOptions().AmazonSns(request => request.MessageDeduplicationId = "native")), default);
        await Assert.That(client.Sent[1].Deduplication).IsEqualTo("native");
    }

    [Test]
    public async Task Batches_split_native_limits_once_per_callback_and_preserve_partial_failure()
    {
        using var client = new SnsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var calls = 0;
        var options = new SendOptions().AmazonSns(_ => calls++);
        var messages = Enumerable.Repeat(new OutgoingMessage("orders", "{}"u8.ToArray(), options: options), 11).ToList();
        var large = Encoding.UTF8.GetBytes(new string('x', 150_000));
        messages.Add(new("orders", large, options: options));
        messages.Add(new("orders", large, options: options));
        messages.Add(new("ordered.fifo", "{}"u8.ToArray(), orderingKey: new WireString("group"), options: options));
        await transport.SendBatchAsync(messages.ToArray(), default);
        await Assert.That(client.Batches.Select(batch => batch.Length).SequenceEqual(new[] { 10, 2, 1, 1 })).IsTrue();
        await Assert.That(calls).IsEqualTo(14);
        client.FailBatch = true;
        AmazonSnsBatchException? failure = null;
        try { await transport.SendBatchAsync(messages.ToArray(), default); }
        catch (AmazonSnsBatchException error) { failure = error; }
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.Response.Successful.Single().Id).IsEqualTo("0");
        await Assert.That(failure.Response.Failed.Single().Id).IsEqualTo("1");
        await Assert.That(failure.BatchOffset).IsEqualTo(0);
        await Assert.That(client.Batches.Count).IsEqualTo(5);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        await Assert.That(client.Sent.Single().Attributes.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Invalid_raw_delivery_attributes_utf8_timers_ordering_and_size_fail_before_publish()
    {
        using var client = new SnsTestClient();
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var headers = Enumerable.Range(0, 11).Select(i => new KeyValuePair<string, WireString>("h" + i, new("v"))).ToArray();
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), headers), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[] { 0xff }), default)).Throws<DecoderFallbackException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), deliverAt: DateTimeOffset.UtcNow.AddMinutes(1)), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), orderingKey: new WireString("group")), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendAsync(new("ordered.fifo", "{}"u8.ToArray()), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[262_145]), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray(), options: new SendOptions().AmazonSns(request => request.PhoneNumber = "+1234")), default)).Throws<InvalidOperationException>();
        await Assert.That(client.Sent.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Concurrent_operations_borrow_independent_storage_until_native_completion()
    {
        using var client = new SnsTestClient { Gate = new(TaskCreationOptions.RunContinuationsAsynchronously) };
        await using var transport = Transport(client);
        await transport.StartAsync(default);
        var first = transport.SendAsync(new("orders", "first"u8.ToArray()), default);
        var second = transport.SendAsync(new("orders", "second"u8.ToArray()), default);
        await Assert.That(ReferenceEquals(client.Requests[0], client.Requests[1])).IsFalse();
        await Assert.That(client.Requests[0].Message).IsEqualTo("first");
        await Assert.That(client.Requests[1].Message).IsEqualTo("second");
        client.Gate.SetResult();
        await first;
        await second;
    }

    [Test]
    public async Task Topology_validate_is_read_only_and_provision_creates_only_missing_resources()
    {
        using var client = new SnsTestClient { Missing = true };
        var options = Options(client);
        options.Topics[0].Attributes = new() { ["DisplayName"] = "Orders" };
        options.Subscriptions.Add(Subscription());
        await using var transport = new SnsSendTransport("sns", client, false, options);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.None, default)).Differences.Count).IsEqualTo(0);
        await Assert.That(client.Lookups).IsEqualTo(0);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, default)).Differences.Count).IsEqualTo(3);
        await Assert.That(client.Creates.Count).IsEqualTo(0);
        await Assert.That(client.Subscribes.Count).IsEqualTo(0);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, default)).Differences.Count).IsEqualTo(1);
        await Assert.That(client.Creates.Count).IsEqualTo(2);
        await Assert.That(client.Subscribes.Count).IsEqualTo(1);
        await Assert.That(client.Subscribes[0]).IsSameReferenceAs(options.Subscriptions[0]);
        await transport.ApplyTopologyAsync(TopologyMode.Provision, default);
        await Assert.That(client.Creates.Count).IsEqualTo(2);
        await Assert.That(client.Subscribes.Count).IsEqualTo(1);
        client.SubscriptionAttributes["RawMessageDelivery"] = "false";
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, default)).Differences.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Declaration_validation_rejects_missing_topics_fifo_mismatch_and_non_raw_subscriptions()
    {
        var options = new AmazonSnsTransportOptions();
        await Assert.That(() => SnsSendTransport.ValidateDeclarations("sns", options, ["missing"])).Throws<InvalidOperationException>();
        options.Topics.Add(new("bad.fifo"));
        await Assert.That(() => SnsSendTransport.ValidateDeclarations("sns", options, [])).Throws<InvalidOperationException>();
        options.Topics.Clear();
        var subscription = Subscription();
        subscription.Attributes["RawMessageDelivery"] = "false";
        options.Subscriptions.Add(subscription);
        await Assert.That(() => SnsSendTransport.ValidateDeclarations("sns", options, [])).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Native_send_telemetry_uses_sns_and_request_id()
    {
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), []);
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activities.Add,
        };
        ActivitySource.AddActivityListener(listener);
        using var client = new SnsTestClient();
        await using var transport = new SnsSendTransport("sns", client, false, Options(client), tracing);
        await transport.StartAsync(default);
        await transport.SendAsync(new("orders", "{}"u8.ToArray()), default);
        await transport.SendBatchAsync(new OutgoingMessage[] { new("orders", "{}"u8.ToArray()) }, default);
        await Assert.That(activities.Count).IsEqualTo(3);
        await Assert.That(activities[0].GetTagItem("messaging.system")).IsEqualTo("aws.sns");
        await Assert.That(activities[0].GetTagItem("aws.request_id")).IsEqualTo("request-1");
        await Assert.That(activities[^1].GetTagItem("aws.request_id")).IsEqualTo("batch-request");
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
        using var client = new SnsTestClient { Gate = new(TaskCreationOptions.RunContinuationsAsynchronously) };
        await using var transport = new SnsSendTransport("test", client, false, Options(client), tracing);
        await transport.StartAsync(default);
        var message = new OutgoingMessage("orders", "{}"u8.ToArray());
        var pending = batch ? transport.SendBatchAsync(new[] { message, message }, default) : transport.SendAsync(message, default);
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(sent).IsEqualTo(batch ? 2L : 1L);
        await Assert.That(durations.Values.Count).IsEqualTo(0);
        if (fail) client.Gate.SetException(new AmazonSimpleNotificationServiceException("failure"));
        else client.Gate.SetResult();
        if (fail) await Assert.That(async () => await pending).Throws<AmazonSimpleNotificationServiceException>();
        else await pending;
        var duration = durations.Values.Single();
        await Assert.That(duration.Tags["messaging.system"]).IsEqualTo("aws.sns");
        await Assert.That(duration.Tags["messaging.destination.name"]).IsEqualTo("orders");
        await Assert.That(duration.Tags.ContainsKey("error.type")).IsEqualTo(fail);
    }

    [Test]
    public async Task Split_batches_count_native_attempts_and_partial_failure_stops_later_batches()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var durations = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var client = new SnsTestClient();
        await using var transport = new SnsSendTransport("test", client, false, Options(client), tracing);
        await transport.StartAsync(default);
        var messages = Enumerable.Repeat(new OutgoingMessage("orders", "{}"u8.ToArray()), 11).ToArray();
        await transport.SendBatchAsync(messages, default);
        await Assert.That(durations.Values.Count).IsEqualTo(2);
        durations.Values.Clear();
        client.FailBatch = true;
        await Assert.That(async () => await transport.SendBatchAsync(messages, default)).Throws<AmazonSnsBatchException>();
        await Assert.That(durations.Values.Single().Tags["error.type"]).IsEqualTo("_OTHER");
        durations.Values.Clear();
        await transport.SendBatchAsync(Array.Empty<OutgoingMessage>(), default);
        await Assert.That(async () => await transport.SendAsync(new("orders", new byte[] { 0xff }), default)).Throws<DecoderFallbackException>();
        await Assert.That(durations.Values.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Native_paginators_find_later_topics_and_subscriptions()
    {
        using var client = new SnsTestClient { Paginate = true };
        var options = Options(client);
        options.Subscriptions.Add(Subscription());
        client.Subscribes.Add(Subscription());
        await using var transport = new SnsSendTransport("sns", client, false, options);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, default)).Differences.Count).IsEqualTo(0);
        await transport.SendAsync(new("ordered.fifo", "{}"u8.ToArray(), orderingKey: new WireString("group")), default);
        await Assert.That(client.Sent.Single().Arn).IsEqualTo(SnsTestClient.Arn("ordered.fifo"));
        await Assert.That(client.Creates.Count).IsEqualTo(0);
        await Assert.That(client.Subscribes.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Conflicting_fifo_identity_bindings_fail_before_native_lookup()
    {
        using var client = new SnsTestClient();
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.AmazonSns("sns", sns =>
        {
            sns.Options.Configure(options =>
            {
                options.SnsClient = client;
                options.Topics.Add(new("ordered.fifo") { Attributes = new() { ["FifoTopic"] = "true" } });
            });
            sns.Send<Event>("ordered.fifo");
            sns.Send<SqsSendTests.Order>("ordered.fifo", route => route.SendOptions = new() { HeaderNames = new("event_") });
        }));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetServices<ITransport>().ToArray()).Throws<InvalidOperationException>();
        await Assert.That(client.Lookups).IsEqualTo(0);
    }

    [Test]
    public async Task Cancellation_and_disposal_stop_before_native_work_and_owned_clients_dispose_once()
    {
        var client = new SnsTestClient();
        var transport = new SnsSendTransport("sns", client, true, Options(client));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await transport.StartAsync(cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(client.Lookups).IsEqualTo(0);
        await transport.DisposeAsync();
        await transport.DisposeAsync();
        await Assert.That(client.Disposals).IsEqualTo(1);
        await Assert.That(async () => await transport.SendAsync(new("orders", "{}"u8.ToArray()), default)).Throws<ObjectDisposedException>();
    }

    internal static AmazonSnsTransportOptions Options(IAmazonSimpleNotificationService client)
    {
        var options = new AmazonSnsTransportOptions { SnsClient = client };
        options.Topics.Add(new("orders"));
        options.Topics.Add(new("ordered.fifo") { Attributes = new() { ["FifoTopic"] = "true" } });
        return options;
    }
    private static SubscribeRequest Subscription() => new()
    {
        TopicArn = SnsTestClient.Arn("orders"), Protocol = "sqs", Endpoint = "arn:aws:sqs:us-east-1:000000000000:orders",
        Attributes = new() { ["RawMessageDelivery"] = "true" }, ReturnSubscriptionArn = true,
    };
    private static SnsSendTransport Transport(IAmazonSimpleNotificationService client) => new("sns", client, false, Options(client));
    [Message("sns.test.event")]
    public sealed record Event;
}

internal sealed record SnsSent(string Arn, string Body, string? Group, string? Deduplication, string? Subject, Dictionary<string, string> Attributes);

internal sealed class SnsTestClient() : AmazonSimpleNotificationServiceClient(new AnonymousAWSCredentials(), new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost:4566" })
{
    internal readonly List<SnsSent> Sent = [];
    internal readonly List<PublishRequest> Requests = [];
    internal readonly List<(string Id, SnsSent Message)[]> Batches = [];
    internal readonly List<CreateTopicRequest> Creates = [];
    internal readonly List<SubscribeRequest> Subscribes = [];
    internal readonly Dictionary<string, string> SubscriptionAttributes = new() { ["RawMessageDelivery"] = "true" };
    internal bool FailBatch;
    internal bool Missing;
    internal bool Paginate;
    internal int Lookups;
    internal int Disposals;
    internal TaskCompletionSource? Gate;
    internal static string Arn(string name) => "arn:aws:sns:us-east-1:000000000000:" + name;

    public override async Task<PublishResponse> PublishAsync(PublishRequest request, CancellationToken cancellationToken = default)
    {
        Requests.Add(request);
        Sent.Add(Snapshot(request));
        if (Gate is not null) await Gate.Task.WaitAsync(cancellationToken);
        return new() { ResponseMetadata = new() { RequestId = "request-1" } };
    }
    public override async Task<PublishBatchResponse> PublishBatchAsync(PublishBatchRequest request, CancellationToken cancellationToken = default)
    {
        Batches.Add(request.PublishBatchRequestEntries.Select(entry => (entry.Id, Snapshot(new()
        {
            TopicArn = request.TopicArn, Message = entry.Message, MessageGroupId = entry.MessageGroupId,
            MessageDeduplicationId = entry.MessageDeduplicationId, Subject = entry.Subject, MessageAttributes = entry.MessageAttributes,
        }))).ToArray());
        if (Gate is not null) await Gate.Task.WaitAsync(cancellationToken);
        return new() { Successful = [new() { Id = "0" }], Failed = FailBatch ? [new() { Id = "1", Code = "InvalidParameter", SenderFault = true }] : [], ResponseMetadata = new() { RequestId = "batch-request" } };
    }
    public override Task<ListTopicsResponse> ListTopicsAsync(ListTopicsRequest request, CancellationToken cancellationToken = default)
    {
        Lookups++;
        var names = Missing ? Creates.Select(topic => topic.Name) : new[] { "orders", "ordered.fifo" };
        if (Paginate) names = request.NextToken is null ? ["orders"] : ["ordered.fifo"];
        return Task.FromResult(new ListTopicsResponse { NextToken = Paginate && request.NextToken is null ? "next" : null, Topics = Missing && Creates.Count == 0 ? null : names.Select(name => new Topic { TopicArn = Arn(name) }).ToList() });
    }
    public override Task<CreateTopicResponse> CreateTopicAsync(CreateTopicRequest request, CancellationToken cancellationToken = default)
    {
        Creates.Add(request);
        return Task.FromResult(new CreateTopicResponse { TopicArn = Arn(request.Name) });
    }
    public override Task<GetTopicAttributesResponse> GetTopicAttributesAsync(GetTopicAttributesRequest request, CancellationToken cancellationToken = default) =>
        Task.FromResult(new GetTopicAttributesResponse { Attributes = new() { ["FifoTopic"] = "true" } });
    public override Task<ListSubscriptionsByTopicResponse> ListSubscriptionsByTopicAsync(ListSubscriptionsByTopicRequest request, CancellationToken cancellationToken = default) =>
        Task.FromResult(new ListSubscriptionsByTopicResponse { NextToken = Paginate && request.NextToken is null ? "next" : null, Subscriptions = (Paginate && request.NextToken is null ? Enumerable.Empty<SubscribeRequest>() : Subscribes).Select(subscription => new Subscription
        { TopicArn = subscription.TopicArn, Endpoint = subscription.Endpoint, Protocol = subscription.Protocol, SubscriptionArn = "arn:subscription" }).ToList() });
    public override Task<SubscribeResponse> SubscribeAsync(SubscribeRequest request, CancellationToken cancellationToken = default)
    {
        Subscribes.Add(request);
        return Task.FromResult(new SubscribeResponse { SubscriptionArn = "arn:subscription" });
    }
    public override Task<GetSubscriptionAttributesResponse> GetSubscriptionAttributesAsync(GetSubscriptionAttributesRequest request, CancellationToken cancellationToken = default) =>
        Task.FromResult(new GetSubscriptionAttributesResponse { Attributes = SubscriptionAttributes });
    protected override void Dispose(bool disposing) { if (disposing) Disposals++; base.Dispose(disposing); }
    private static SnsSent Snapshot(PublishRequest request) => new(request.TopicArn, request.Message, request.MessageGroupId,
        request.MessageDeduplicationId, request.Subject, request.MessageAttributes?.ToDictionary(item => item.Key, item => item.Value.StringValue) ?? []);
}
