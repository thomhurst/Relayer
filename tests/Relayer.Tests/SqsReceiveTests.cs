using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Relayer.AmazonSqs;
using Relayer.HealthChecks;

namespace Relayer.Tests;

public sealed class SqsReceiveTests
{
    [Test]
    public async Task Binding_reuses_body_and_native_objects_and_combines_copy_and_receive_counts()
    {
        using var client = new ReceiveClient();
        using var delivery = new SqsDelivery(client, "url");
        var native = Message("{\"Value\":1}", 3);
        native.MessageAttributes[FailureHeaders.Redeliveries] = new() { DataType = "Number", StringValue = "2" };
        delivery.Prepare(native, TimeProvider.System.GetTimestamp(), "request");
        await Assert.That(Encoding.UTF8.GetString(SqsMessageBinding.GetBody(delivery).FirstSpan)).IsEqualTo(native.Body);
        await Assert.That(delivery.Message).IsSameReferenceAs(native);
        await Assert.That(SqsMessageBinding.GetDeliveryCount(delivery)).IsEqualTo(3);
        await Assert.That(SqsMessageBinding.TryGetHeader(delivery, FailureHeaders.Redeliveries, out var retries)).IsTrue();
        await Assert.That(retries.ToString()).IsEqualTo("4");
        await Assert.That(SqsMessageBinding.TryGetHeader(delivery, "ce_type", out var type)).IsTrue();
        await Assert.That(type.ToString()).IsEqualTo("sqs.receive;v=1");
        delivery.Clear();
        delivery.Prepare(Message("short", 1), TimeProvider.System.GetTimestamp(), null);
        await Assert.That(SqsMessageBinding.GetBody(delivery).Length).IsEqualTo(5);
        SqsMessageBinding.TryGetHeader(delivery, FailureHeaders.Redeliveries, out retries);
        await Assert.That(retries.ToString()).IsEqualTo("0");
    }

    [Test]
    public async Task Receive_errors_retry_without_permanently_losing_worker()
    {
        using var client = new ReceiveClient { PollFailure = new AmazonSQSException("temporarily denied") };
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var consumer = new SqsConsumer(Declaration(), client, "url", (_, _) =>
        { handled.TrySetResult(); return default; });
        await consumer.StartAsync(default);
        await client.Deliveries.Writer.WriteAsync(Message("body", 1));
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Polls >= 2).IsTrue();
        await consumer.StopAsync(default);
    }

    [Test]
    public async Task Visibility_uses_original_monotonic_receive_deadline_and_never_deletes_on_failed_retry()
    {
        using var client = new ReceiveClient();
        var time = new ManualTime();
        using var delivery = new SqsDelivery(client, "url", time);
        delivery.Prepare(Message("body", 1), time.GetTimestamp(), "request");
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var settlement = new SqsSettlement(tracing, new() { System = "aws_sqs", Destination = "orders" });
        time.Advance(TimeSpan.FromHours(11));
        await settlement.SettleAsync(delivery, Disposition.Redeliver(TimeSpan.FromSeconds(1.1)), default);
        await Assert.That(client.Visibility.ToArray()).IsEquivalentTo(new[] { 2 });
        time.Advance(TimeSpan.FromMinutes(59));
        await Assert.That(async () => await settlement.SettleAsync(delivery, Disposition.Redeliver(TimeSpan.FromMinutes(2)), default)).Throws<InvalidOperationException>();
        await Assert.That(client.Deletes).IsEqualTo(0);
        await settlement.SettleAsync(delivery, Disposition.Complete, default);
        await Assert.That(client.Deletes).IsEqualTo(1);
    }

    [Test]
    public async Task Stop_cancels_long_polls_but_drains_work_and_reports_oldest_active_worker()
    {
        using var client = new ReceiveClient();
        var time = new ManualTime();
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSecond = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        await using var receiver = new SqsConsumer(Declaration(2), client, "url", async (_, token) =>
        {
            var current = Interlocked.Increment(ref count);
            if (current == 1) { first.SetResult(); await releaseFirst.Task.WaitAsync(token); }
            else { second.SetResult(); await releaseSecond.Task.WaitAsync(token); }
        }, time) { TrackHealth = true };
        await receiver.StartAsync(default);
        await client.Deliveries.Writer.WriteAsync(Message("one", 1));
        await first.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var oldest = receiver.GetHealthState()!.Value.LastProgressAt;
        time.Advance(TimeSpan.FromMinutes(1));
        await client.Deliveries.Writer.WriteAsync(Message("two", 1));
        await second.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(receiver.GetHealthState()!.Value.LastProgressAt).IsEqualTo(oldest);
        var stop = receiver.StopAsync(default);
        await Assert.That(stop.IsCompleted).IsFalse();
        releaseSecond.SetResult();
        await Assert.That(receiver.GetHealthState()!.Value.LastProgressAt).IsEqualTo(oldest);
        releaseFirst.SetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(receiver.GetHealthState()).IsNull();
        await receiver.StartAsync(default);
        await Assert.That(receiver.GetHealthState()!.Value.IsIdle).IsTrue();
        await receiver.StopAsync(default);
    }

    [Test]
    public async Task Named_health_is_read_only_preserves_connectivity_failure_and_cancellation()
    {
        using var client = new ReceiveClient();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRelayerAmazonSqsHealthProviders("ExactSqs");
        services.AddRelayerAmazonSqsHealthProviders("ExactSqs");
        services.AddRelayer(builder => builder.AmazonSqs("ExactSqs", sqs =>
        {
            sqs.Options.Configure(options => { options.SqsClient = client; options.Queues.Add(new("orders")); });
        }));
        await using var provider = services.BuildServiceProvider();
        var probe = provider.GetRequiredKeyedService<ITransportHealthProbe>("ExactSqs");
        await Assert.That(provider.GetKeyedService<ITransportHealthProbe>("exactsqs")).IsNull();
        await Assert.That((await probe.CheckAsync(default)).Status).IsEqualTo(HealthStatus.Healthy);
        var failure = new HttpRequestException("disconnected");
        client.ConnectivityFailure = failure;
        var failed = await probe.CheckAsync(default);
        await Assert.That(failed.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(failed.Exception).IsSameReferenceAs(failure);
        using var cancellation = new CancellationTokenSource(); cancellation.Cancel();
        await Assert.That(async () => await probe.CheckAsync(cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(client.Deletes).IsEqualTo(0);
        await Assert.That(client.Polls).IsEqualTo(0);
    }

    [Test]
    public async Task Redrive_must_leave_room_for_final_failure_metadata_copy()
    {
        using var client = new ReceiveClient();
        var options = new AmazonSqsTransportOptions { SqsClient = client };
        options.Queues.Add(new("orders"));
        await using var transport = new SqsSendTransport("sqs", client, false, options);
        await transport.ResolveUrlsAsync(default);
        client.Redrive = "{\"maxReceiveCount\":\"1\",\"deadLetterTargetArn\":\"arn\"}";
        var declaration = new ConsumerDefinition<AmazonSqsConsumerOptions>("orders", new(), [], 1, 1, default, failure: new() { RedeliveryDelays = new[] { TimeSpan.Zero } });
        await Assert.That(async () => await transport.ValidateRedriveAsync(declaration, default)).Throws<InvalidOperationException>();
        client.Redrive = "{\"maxReceiveCount\":2,\"deadLetterTargetArn\":\"arn\"}";
        await transport.ValidateRedriveAsync(declaration, default);
    }

    [Test]
    public async Task Native_request_validation_fails_before_any_poll_and_aborted_drain_cancels_work()
    {
        using var client = new ReceiveClient();
        var invalid = new ConsumerDefinition<AmazonSqsConsumerOptions>("orders", new()
            { ConfigureReceive = request => request.MaxNumberOfMessages = 10 }, [], 1, 1, default);
        await using (var receiver = new SqsConsumer(invalid, client, "url", static (_, _) => default))
            await Assert.That(async () => await receiver.StartAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(client.Polls).IsEqualTo(0);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var consumer = new SqsConsumer(Declaration(), client, "url", async (_, token) =>
        {
            entered.SetResult();
            try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
            finally
            {
                canceled.SetResult();
                // Keep the drain incomplete: WaitAsync may return success when workers
                // finish before it observes an already-canceled deadline.
                await release.Task;
            }
        });
        await consumer.StartAsync(default);
        await client.Deliveries.Writer.WriteAsync(Message("body", 1));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        using var deadline = new CancellationTokenSource(); deadline.Cancel();
        try
        {
            await Assert.That(async () => await consumer.StopAsync(deadline.Token)).Throws<OperationCanceledException>();
        }
        finally { release.SetResult(); }
        await canceled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(async () => await consumer.StartAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(client.Deletes).IsEqualTo(0);
    }

    internal static ConsumerDefinition<AmazonSqsConsumerOptions> Declaration(int concurrency = 1) =>
        new("orders", new(), [], concurrency, 1, default, failure: new() { DeadLetterEndpoint = "dead" });
    internal static Message Message(string body, int count) => new()
    {
        Body = body, ReceiptHandle = Guid.NewGuid().ToString(),
        Attributes = new() { ["ApproximateReceiveCount"] = count.ToString(System.Globalization.CultureInfo.InvariantCulture) },
        MessageAttributes = new() { ["ce_id"] = new() { DataType = "String", StringValue = "id" }, ["ce_type"] = new() { DataType = "String", StringValue = "sqs.receive;v=1" } },
    };
    internal sealed class ManualTime : TimeProvider
    {
        private long _ticks = DateTimeOffset.UtcNow.UtcTicks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => _ticks;
        public override DateTimeOffset GetUtcNow() => new(_ticks, TimeSpan.Zero);
        internal void Advance(TimeSpan span) => _ticks += span.Ticks;
    }
    internal sealed class ReceiveClient() : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost", AuthenticationRegion = "us-east-1" })
    {
        internal readonly Channel<Message> Deliveries = Channel.CreateUnbounded<Message>();
        internal readonly ConcurrentQueue<int> Visibility = new();
        internal int Deletes;
        internal int Sends;
        internal Exception? PollFailure;
        internal int Polls;
        internal Exception? ConnectivityFailure;
        internal string? Redrive;
        internal Exception? SendFailure;
        internal readonly TaskCompletionSource NextPoll = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Sends);
            return SendFailure is { } error ? Task.FromException<SendMessageResponse>(error) : Task.FromResult(new SendMessageResponse());
        }
        public override async Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref Polls) == 2) NextPoll.TrySetResult();
            if (Interlocked.Exchange(ref PollFailure, null) is { } failure) throw failure;
            return new() { Messages = [await Deliveries.Reader.ReadAsync(cancellationToken)] };
        }
        public override Task<DeleteMessageResponse> DeleteMessageAsync(DeleteMessageRequest request, CancellationToken cancellationToken = default)
        { Interlocked.Increment(ref Deletes); return Task.FromResult(new DeleteMessageResponse()); }
        public override Task<ChangeMessageVisibilityResponse> ChangeMessageVisibilityAsync(ChangeMessageVisibilityRequest request, CancellationToken cancellationToken = default)
        { Visibility.Enqueue(request.VisibilityTimeout!.Value); return Task.FromResult(new ChangeMessageVisibilityResponse()); }
        public override Task<GetQueueUrlResponse> GetQueueUrlAsync(string name, CancellationToken cancellationToken = default)
        { cancellationToken.ThrowIfCancellationRequested(); return Task.FromResult(new GetQueueUrlResponse { QueueUrl = "http://localhost/" + name }); }
        public override Task<GetQueueAttributesResponse> GetQueueAttributesAsync(GetQueueAttributesRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (ConnectivityFailure is { } failure) throw failure;
            return Task.FromResult(new GetQueueAttributesResponse { Attributes = Redrive is { } policy ? new() { ["RedrivePolicy"] = policy } : [] });
        }
    }
}
