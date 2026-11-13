using Relayer.Transports;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Options;
using Relayer.AmazonSqs;

namespace Relayer.Tests;

public sealed class SqsBatchReceiveTests
{
    [Test]
    [Arguments(0)]
    [Arguments(11)]
    public async Task Invalid_standard_receive_size_fails_before_poll(int size)
    {
        using var client = new BatchClient();
        await using var consumer = new SqsConsumer(Declaration(size), client, "url", static (_, _) => default);
        await Assert.That(async () => await consumer.StartAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(client.Polls).IsEqualTo(0);
    }

    [Test]
    public async Task Fifo_still_requires_one_message_per_poll()
    {
        using var client = new BatchClient();
        await using var consumer = new SqsConsumer(Declaration(10, "orders.fifo"), client, "url", static (_, _) => default);
        await Assert.That(async () => await consumer.StartAsync(default)).Throws<InvalidOperationException>();
        await Assert.That(client.Polls).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Batch_results_complete_only_successful_entries_and_keep_borrowed_state(bool wholeRequestFailure)
    {
        using var client = new BatchClient { FailRequest = wholeRequestFailure };
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var settlement = new SqsSettlement(tracing, new() { System = "aws_sqs", Destination = "orders" });
        var completed = new ConcurrentQueue<string>();
        var failed = new ConcurrentQueue<string>();
        var deliveries = new ConcurrentQueue<SqsDelivery>();
        await using var consumer = new SqsConsumer(Declaration(10), client, "url", async (delivery, token) =>
        {
            deliveries.Enqueue(delivery);
            var body = delivery.Message.Body;
            try { await settlement.SettleAsync(delivery, Disposition.Complete, token); completed.Enqueue(body); }
            catch { failed.Enqueue(body); throw; }
            finally
            {
                await Assert.That(delivery.Message.Body).IsEqualTo(body);
                await Assert.That(Encoding.UTF8.GetString(delivery.Body.Span)).IsEqualTo(body);
            }
        });
        await consumer.StartAsync(default);
        await client.Batches.Writer.WriteAsync([Message("one"), Message("two"), Message("three")]);
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await consumer.StopAsync(default);
        await Assert.That(deliveries.Distinct().Count()).IsEqualTo(3);
        await Assert.That(completed.ToArray()).IsEquivalentTo(wholeRequestFailure ? Array.Empty<string>() : ["one"]);
        await Assert.That(failed.Count).IsEqualTo(wholeRequestFailure ? 3 : 2);
        await Assert.That(client.Deleted.ToArray()).IsEquivalentTo(new[] { "one", "two", "three" });
    }

    [Test]
    public async Task Graceful_stop_drains_returned_batch_and_never_deletes_failed_work()
    {
        using var client = new BatchClient { AllSucceed = true };
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var settlement = new SqsSettlement(tracing, new() { System = "aws_sqs", Destination = "orders" });
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var visited = new ConcurrentQueue<string>();
        await using var consumer = new SqsConsumer(Declaration(10), client, "url", async (delivery, token) =>
        {
            var body = delivery.Message.Body;
            visited.Enqueue(body);
            if (body == "one") { entered.SetResult(); await release.Task.WaitAsync(token); }
            if (body == "copy-failed") throw new InvalidOperationException("durable failure copy failed");
            await settlement.SettleAsync(delivery, Disposition.Complete, token);
        });
        await consumer.StartAsync(default);
        await client.Batches.Writer.WriteAsync([Message("one"), Message("copy-failed"), Message("three")]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var stopping = consumer.StopAsync(default);
        await Assert.That(stopping.IsCompleted).IsFalse();
        await Assert.That(client.Deleted.Count).IsEqualTo(0);
        release.SetResult();
        await stopping.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(visited.ToArray()).IsEquivalentTo(new[] { "one", "copy-failed", "three" });
        await Assert.That(client.Deleted.ToArray()).IsEquivalentTo(new[] { "one", "three" });
    }

    [Test]
    public async Task Abort_releases_pending_settlement_without_deleting_unprocessed_messages()
    {
        using var client = new BatchClient { AllSucceed = true };
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var settlement = new SqsSettlement(tracing, new() { System = "aws_sqs", Destination = "orders" });
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var visited = new ConcurrentQueue<string>();
        await using var consumer = new SqsConsumer(Declaration(10), client, "url", async (delivery, token) =>
        {
            visited.Enqueue(delivery.Message.Body);
            if (delivery.Message.Body == "two")
            { entered.SetResult(); await Task.Delay(Timeout.InfiniteTimeSpan, token); }
            await settlement.SettleAsync(delivery, Disposition.Complete, token);
        });
        await consumer.StartAsync(default);
        await client.Batches.Writer.WriteAsync([Message("one"), Message("two"), Message("three")]);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        using var deadline = new CancellationTokenSource(); deadline.Cancel();
        try { await consumer.StopAsync(deadline.Token); }
        catch (OperationCanceledException) { }
        await consumer.StopAsync(default).WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(visited.ToArray()).IsEquivalentTo(new[] { "one", "two" });
        await Assert.That(client.Deleted.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Reused_slots_preserve_parent_tracing_until_native_delete_completes()
    {
        using var client = new BatchClient { AllSucceed = true };
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var stopped = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => ReferenceEquals(source, tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = stopped.Add,
        };
        ActivitySource.AddActivityListener(listener);
        var settlement = new SqsSettlement(tracing, new() { System = "aws_sqs", Destination = "orders" });
        using var parent = new Activity("parent").Start();
        var completions = 0;
        await using var consumer = new SqsConsumer(Declaration(10), client, "url", async (delivery, token) =>
        {
            var pending = settlement.SettleAsync(delivery, Disposition.Complete, token);
            await Assert.That(Activity.Current).IsSameReferenceAs(parent);
            await Assert.That(pending.IsCompleted).IsFalse();
            await pending;
            completions++;
        });
        using var batch = new SqsConsumer.SqsReceiveBatch(consumer, 10);
        for (var i = 0; i < 3; i++)
            await batch.ProcessAsync(new() { Messages = [Message("one"), Message("two")] }, TimeProvider.System.GetTimestamp(), default);
        await Assert.That(completions).IsEqualTo(6);
        await Assert.That(stopped.Count).IsEqualTo(6);
        foreach (var activity in stopped)
        {
            await Assert.That(activity.ParentId).IsEqualTo(parent.Id);
            await Assert.That(activity.Duration > TimeSpan.Zero).IsTrue();
        }
        await Assert.That(Activity.Current).IsSameReferenceAs(parent);
    }

    internal static ConsumerDefinition<AmazonSqsConsumerOptions> Declaration(int size, string source = "orders") =>
        new(source, new() { ConfigureReceive = request => request.MaxNumberOfMessages = size }, [], 1, 1, default,
            failure: new() { DeadLetterEndpoint = "dead" });
    internal static Message Message(string body) => new() { Body = body, ReceiptHandle = body, Attributes = [], MessageAttributes = [] };

    internal sealed class BatchClient() : AmazonSQSClient(new BasicAWSCredentials("test", "test"),
        new AmazonSQSConfig { ServiceURL = "http://localhost", AuthenticationRegion = "us-east-1" })
    {
        internal readonly Channel<List<Message>> Batches = Channel.CreateUnbounded<List<Message>>();
        internal readonly ConcurrentQueue<string> Deleted = new();
        internal readonly TaskCompletionSource NextPoll = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Polls;
        internal bool FailRequest;
        internal bool AllSucceed;
        public override async Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref Polls) > 1) NextPoll.TrySetResult();
            return new() { Messages = await Batches.Reader.ReadAsync(cancellationToken) };
        }
        public override Task<DeleteMessageBatchResponse> DeleteMessageBatchAsync(DeleteMessageBatchRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var entry in request.Entries) Deleted.Enqueue(entry.ReceiptHandle);
            if (FailRequest) return Task.FromException<DeleteMessageBatchResponse>(new AmazonSQSException("request failed"));
            return Task.FromResult(new DeleteMessageBatchResponse
            {
                Successful = request.Entries.Where((_, index) => AllSucceed || index == 0).Select(entry => new DeleteMessageBatchResultEntry { Id = entry.Id }).ToList(),
                // Third entry is deliberately omitted: absence must never count as success.
                Failed = AllSucceed || request.Entries.Count < 2 ? [] : [new() { Id = request.Entries[1].Id, Code = "ReceiptHandleIsInvalid", Message = "failed" }],
            });
        }
    }
}
