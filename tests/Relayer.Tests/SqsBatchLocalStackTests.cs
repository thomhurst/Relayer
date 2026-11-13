using Relayer.Transports;
using System.Collections.Concurrent;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Options;
using Relayer.AmazonSqs;

namespace Relayer.Tests;

public sealed class SqsBatchLocalStackTests
{
    [Test]
    [Arguments("partial")]
    [Arguments("copy")]
    [Arguments("drain")]
    [Arguments("abort")]
    public async Task Native_batches_preserve_failed_and_unprocessed_deliveries(string scenario)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        var endpoint = await SharedBrokers.LocalStackUrlAsync(token);
        using var client = new BatchClient(endpoint, scenario == "partial");
        var name = TestContext.Current!.Isolation.GetIsolatedName("batch");
        var url = (await client.CreateQueueAsync(new CreateQueueRequest(name)
            { Attributes = new() { ["VisibilityTimeout"] = "60" } }, token)).QueueUrl;
        try
        {
            await client.SendMessageBatchAsync(new() { QueueUrl = url, Entries = [new("a", "a"), new("b", "b"), new("c", "c")] }, token);
            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
            var settlement = new SqsSettlement(tracing, new() { System = "aws_sqs", Destination = name });
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var visited = new ConcurrentQueue<Message>();
            var completed = new ConcurrentQueue<string>();
            var failures = new ConcurrentQueue<Exception>();
            var attempts = 0;
            await using var consumer = new SqsConsumer(SqsBatchReceiveTests.Declaration(10, name), client, url, async (delivery, cancellation) =>
            {
                visited.Enqueue(delivery.Message);
                var index = Interlocked.Increment(ref attempts);
                try
                {
                    if (index == 2 && scenario is "drain" or "abort")
                    {
                        entered.TrySetResult();
                        await release.Task.WaitAsync(cancellation);
                    }
                    if (index == 2 && scenario == "copy")
                    {
                        // A real failed durable poison copy must never enter the delete batch.
                        await client.SendMessageAsync(new SendMessageRequest
                            { QueueUrl = endpoint + "/000000000000/missing-" + name, MessageBody = delivery.Message.Body }, cancellation);
                    }
                    await settlement.SettleAsync(delivery, Disposition.Complete, cancellation);
                    completed.Enqueue(delivery.Message.Body);
                }
                catch (Exception error) { failures.Enqueue(error); throw; }
            });
            await consumer.StartAsync(token);
            Task stopping;
            if (scenario is "drain" or "abort")
            {
                await entered.Task.WaitAsync(token);
                using var abort = new CancellationTokenSource();
                if (scenario == "abort") abort.Cancel();
                stopping = consumer.StopAsync(abort.Token);
                if (scenario == "drain")
                {
                    await Assert.That(stopping.IsCompleted).IsFalse();
                    release.SetResult();
                }
                try { await stopping.WaitAsync(token); }
                catch (OperationCanceledException) when (scenario == "abort") { }
                await consumer.StopAsync(default).WaitAsync(token);
            }
            else
            {
                await client.Deleting.Task.WaitAsync(token);
                stopping = consumer.StopAsync(token);
                client.ReleaseDelete.SetResult();
                await stopping;
            }
            await Assert.That(client.FirstReceiveCount).IsEqualTo(3);
            await Assert.That(completed.Count).IsEqualTo(scenario switch { "partial" or "copy" => 2, "drain" => 3, _ => 0 });
            await Assert.That(visited.Count).IsEqualTo(scenario == "abort" ? 2 : 3);
            if (scenario is "partial" or "copy") await Assert.That(failures.Count).IsEqualTo(1);
            // Make retained deliveries visible without waiting for native expiration.
            foreach (var message in client.Received)
                if (!completed.Contains(message.Body))
                    await client.ChangeMessageVisibilityAsync(url, message.ReceiptHandle, 0, token);
            var remaining = await client.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = url, MaxNumberOfMessages = 10, WaitTimeSeconds = 1 }, token);
            await Assert.That(remaining.Messages?.Count ?? 0).IsEqualTo(scenario switch { "partial" or "copy" => 1, "abort" => 3, _ => 0 });
        }
        finally { client.ReleaseDelete.TrySetResult(); await client.DeleteQueueAsync(url, CancellationToken.None); }
    }

    private sealed class BatchClient(string endpoint, bool partial) : AmazonSQSClient(new BasicAWSCredentials("test", "test"),
        new AmazonSQSConfig { ServiceURL = endpoint, AuthenticationRegion = "us-east-1", MaxErrorRetry = 0 })
    {
        internal readonly TaskCompletionSource Deleting = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource ReleaseDelete = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly List<Message> Received = [];
        internal int FirstReceiveCount;
        public override async Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default)
        {
            var response = await base.ReceiveMessageAsync(request, cancellationToken);
            if (Received.Count == 0 && response.Messages is { Count: > 0 })
            { FirstReceiveCount = response.Messages.Count; Received.AddRange(response.Messages); }
            return response;
        }
        public override async Task<DeleteMessageBatchResponse> DeleteMessageBatchAsync(DeleteMessageBatchRequest request, CancellationToken cancellationToken = default)
        {
            Deleting.TrySetResult();
            // Drain tests do not need the external barrier.
            if (partial || request.Entries.Count == 2) await ReleaseDelete.Task.WaitAsync(cancellationToken);
            if (partial)
            {
                // Ask the real emulator for a mixed success/failure response.
                request.Entries[0].ReceiptHandle = "invalid-receipt";
            }
            return await base.DeleteMessageBatchAsync(request, cancellationToken);
        }
    }
}
