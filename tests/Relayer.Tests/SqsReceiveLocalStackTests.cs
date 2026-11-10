using System.Collections.Concurrent;
using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Relayer.AmazonSqs;
using Relayer.HealthChecks;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class SqsReceiveLocalStackTests
{
    [Test]
    [Arguments(false, "ce_")]
    [Arguments(true, "ce_")]
    [Arguments(false, "event_")]
    [Arguments(true, "event_")]
    public async Task Production_receiver_retries_poison_copies_then_deletes_and_reports_native_health(bool fifo, string prefix)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await SqsBroker.StartAsync(token);
        using var client = broker.Client();
        var source = TestContext.Current!.Isolation.GetIsolatedName("receiver") + (fifo ? ".fifo" : "");
        var dead = TestContext.Current!.Isolation.GetIsolatedName("dead") + (fifo ? ".fifo" : "");
        var names = new HeaderNames(prefix);
        var control = new Control();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(control);
        builder.Services.AddRelayerAmazonSqsHealthProviders("ExactSqs");
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.Handler<Handler>();
            relayer.AmazonSqs("ExactSqs", sqs =>
            {
                sqs.TopologyMode = TopologyMode.Provision;
                sqs.Options.Configure(options =>
                {
                    options.SqsClient = client;
                    options.Queues.Add(Queue(source, fifo)); options.Queues.Add(Queue(dead, fifo));
                });
                sqs.Consume(source, consumer =>
                {
                    consumer.Handler<Handler>();
                    consumer.Options.HeaderNames = names;
                    consumer.Options.ConfigureReceive = request => { request.WaitTimeSeconds = 1; request.VisibilityTimeout = 60; };
                    consumer.Options.Failure = new() { DeadLetterEndpoint = dead, RedeliveryDelays = new[] { TimeSpan.FromSeconds(1) }, Jitter = 0 };
                });
            });
        });
        using var host = builder.Build();
        try
        {
            await host.StartAsync(token);
            var transport = host.Services.GetServices<ITransport>().Single(item => item.Name == "ExactSqs");
            var probe = host.Services.GetRequiredKeyedService<ITransportHealthProbe>("ExactSqs");
            var liveness = host.Services.GetRequiredKeyedService<IConsumerHealthProvider>("ExactSqs");
            var declaration = host.Services.GetRequiredService<RelayerConfiguration>().Transports.Single().Consumers.Single();
            await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Healthy);
            await Assert.That((await liveness.GetStateAsync(declaration, token))!.Value.IsIdle).IsTrue();
            async Task Send(int value)
            {
                var headers = new KeyValuePair<string, WireString>[]
                {
                    new(names.Id, new("event-" + value)), new(names.Type, new("sqs.receive;v=1")),
                    new("content-type", new("application/json")), new("traceparent", new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")),
                };
                await transport.SendAsync(new(source, Encoding.UTF8.GetBytes("{\"Value\":" + value + "}"), headers,
                    fifo ? new WireString("group") : null,
                    options: new SendOptions().AmazonSqs(request =>
                    {
                        request.MessageGroupId = "group";
                        request.MessageDeduplicationId = fifo ? "event-" + value : null;
                        request.MessageAttributes["native-binary"] = new() { DataType = "Binary", BinaryValue = new MemoryStream(new byte[] { 1, 2, 3 }) };
                        request.MessageAttributes["native-number"] = new() { DataType = "Number", StringValue = "42" };
                    })), token);
            }
            await Send(1);
            var deadUrl = (await client.GetQueueUrlAsync(dead, token)).QueueUrl;
            var poisoned = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            { QueueUrl = deadUrl, WaitTimeSeconds = 10, MessageAttributeNames = ["All"], MessageSystemAttributeNames = ["All"] }, token);
            var copy = poisoned.Messages.Single();
            await Assert.That(copy.Attributes["MessageGroupId"]).IsEqualTo("group");
            await Assert.That(copy.Body).IsEqualTo("{\"Value\":1}");
            await Assert.That(copy.MessageAttributes[names.Id].StringValue).IsEqualTo("event-1");
            await Assert.That(copy.MessageAttributes["native-binary"].BinaryValue.ToArray()).IsEquivalentTo(new byte[] { 1, 2, 3 });
            await Assert.That(copy.MessageAttributes["native-number"].DataType).IsEqualTo("Number");
            await Assert.That(copy.MessageAttributes["native-number"].StringValue).IsEqualTo("42");
            await Assert.That(copy.MessageAttributes[FailureHeaders.History].StringValue).Contains("poison");
            await Assert.That(copy.MessageAttributes[FailureHeaders.Redeliveries].StringValue).IsEqualTo("1");
            await Assert.That(control.Attempts.ToArray()).IsEquivalentTo(new[] { 1, 2 });
            await client.DeleteMessageAsync(deadUrl, copy.ReceiptHandle, token);
            if (fifo)
            {
                // A malformed envelope still needs native deduplication on a FIFO poison copy.
                var sourceUrlForPoison = (await client.GetQueueUrlAsync(source, token)).QueueUrl;
                await client.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = sourceUrlForPoison, MessageBody = "malformed", MessageGroupId = "malformed-group",
                    MessageDeduplicationId = "native-poison-id",
                }, token);
                var malformed = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = deadUrl, WaitTimeSeconds = 10, MessageAttributeNames = ["All"], MessageSystemAttributeNames = ["All"],
                }, token);
                var malformedCopy = malformed.Messages.Single();
                await Assert.That(malformedCopy.Body).IsEqualTo("malformed");
                await Assert.That(malformedCopy.Attributes["MessageDeduplicationId"]).IsEqualTo("native-poison-id");
                await Assert.That(malformedCopy.MessageAttributes.ContainsKey(FailureHeaders.History)).IsTrue();
                await client.DeleteMessageAsync(deadUrl, malformedCopy.ReceiptHandle, token);
            }
            if (fifo)
            {
                var sourceUrlForTyped = (await client.GetQueueUrlAsync(source, token)).QueueUrl;
                foreach (var (dataType, identity) in new[] { ("Number", "42"), ("String.eventId", "42"), ("String", " "), ("String", "event 1"), ("String", new string('a', 129)), ("String", "évent") })
                {
                    var nativeId = "native-typed-" + Guid.NewGuid().ToString("N");
                    await client.SendMessageAsync(new SendMessageRequest
                    {
                        QueueUrl = sourceUrlForTyped, MessageBody = "malformed", MessageGroupId = "typed-group",
                        MessageDeduplicationId = nativeId,
                        MessageAttributes = new() { [names.Id] = new() { DataType = dataType, StringValue = identity } },
                    }, token);
                    var typed = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                    {
                        QueueUrl = deadUrl, WaitTimeSeconds = 10, MessageAttributeNames = ["All"], MessageSystemAttributeNames = ["All"],
                    }, token);
                    var typedCopy = typed.Messages.Single();
                    await Assert.That(typedCopy.Attributes["MessageDeduplicationId"]).IsEqualTo(nativeId);
                    await Assert.That(typedCopy.MessageAttributes[names.Id].DataType).IsEqualTo(dataType);
                    await Assert.That(typedCopy.MessageAttributes[names.Id].StringValue).IsEqualTo(identity);
                    await Assert.That(typedCopy.MessageAttributes.ContainsKey(FailureHeaders.History)).IsTrue();
                    await client.DeleteMessageAsync(deadUrl, typedCopy.ReceiptHandle, token);
                }
            }
            await Send(2);
            await control.Healthy.Task.WaitAsync(token);
            await Send(3);
            await control.Stalled.Task.WaitAsync(token);
            var active = await liveness.GetStateAsync(declaration, token);
            await Assert.That(active!.Value.IsIdle).IsFalse();
            var startedAt = active.Value.LastProgressAt;
            await Assert.That((await liveness.GetStateAsync(declaration, token))!.Value.LastProgressAt).IsEqualTo(startedAt);
            var stopping = host.StopAsync(token);
            await Assert.That(stopping.IsCompleted).IsFalse();
            control.Release.SetResult();
            await stopping;
            var sourceUrl = (await client.GetQueueUrlAsync(source, token)).QueueUrl;
            var attributes = await client.GetQueueAttributesAsync(sourceUrl, ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"], token);
            await Assert.That(attributes.Attributes["ApproximateNumberOfMessages"]).IsEqualTo("0");
            await Assert.That(attributes.Attributes["ApproximateNumberOfMessagesNotVisible"]).IsEqualTo("0");
            await broker.PauseAsync(token);
            await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Unhealthy);
            await broker.ResumeAsync(token);
            await Assert.That((await probe.CheckAsync(token)).Status).IsEqualTo(HealthStatus.Healthy);
        }
        finally { control.Release.TrySetResult(); await host.StopAsync(CancellationToken.None); }
    }

    private static CreateQueueRequest Queue(string name, bool fifo) => new(name) { Attributes = fifo ? new() { ["FifoQueue"] = "true" } : [] };
    internal sealed class Control
    {
        internal readonly ConcurrentQueue<int> Attempts = new();
        internal readonly TaskCompletionSource Healthy = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Stalled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    [Message("sqs.receive")]
    public sealed record Payload(int Value);
    internal sealed class Handler(Control control) : IHandler<Payload>
    {
        public async ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            if (message.Value == 1) { control.Attempts.Enqueue(context.Attempt); throw new InvalidOperationException("poison"); }
            if (message.Value == 2) { control.Healthy.TrySetResult(); return; }
            control.Stalled.TrySetResult();
            await control.Release.Task.WaitAsync(context.CancellationToken);
        }
    }
}
