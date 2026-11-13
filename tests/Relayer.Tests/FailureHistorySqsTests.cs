using System.Text;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class FailureHistorySqsTests
{
    [Test]
    public async Task Long_poison_history_reaches_a_size_limited_native_queue_before_source_deletion()
    {
        // Shared provisioning has its own five-minute budget; message assertions start after readiness.
        var serviceUrl = await SharedBrokers.LocalStackUrlAsync(CancellationToken.None);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
        {
            ServiceURL = serviceUrl, AuthenticationRegion = "us-east-1", MaxErrorRetry = 0,
        });
        var source = TestContext.Current!.Isolation.GetIsolatedName("history-source");
        var dead = TestContext.Current!.Isolation.GetIsolatedName("history-dead");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AmazonSqs("history", sqs =>
            {
                sqs.TopologyMode = TopologyMode.Provision;
                sqs.Options.Configure(options =>
                {
                    options.SqsClient = client;
                    options.Queues.Add(new(source));
                    options.Queues.Add(new(dead) { Attributes = new() { ["MaximumMessageSize"] = "4096" } });
                });
                sqs.Consume(source, consumer =>
                {
                    consumer.Handler<Handler>();
                    consumer.Options.ConfigureReceive = request => request.WaitTimeSeconds = 1;
                    consumer.Failures(new() { DeadLetterEndpoint = dead, RetryDelays = new TimeSpan[31], Jitter = 0 });
                });
            });
        });
        using var host = builder.Build();
        await host.StartAsync(token);
        try
        {
            var transport = host.Services.GetServices<ITransport>().Single(t => t.Name == "history");
            await transport.SendAsync(new(source, "{}"u8.ToArray(), new KeyValuePair<string, WireString>[]
            {
                new("ce_id", new("poison")), new("ce_type", new("failure.history.sqs;v=1")),
            }), token);
            var deadUrl = (await client.GetQueueUrlAsync(dead, token)).QueueUrl;
            var result = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            { QueueUrl = deadUrl, WaitTimeSeconds = 20, MessageAttributeNames = ["All"] }, token);
            var copy = result.Messages.Single();
            await Assert.That(copy.Body).IsEqualTo("{}");
            await Assert.That(copy.MessageAttributes[FailureHeaders.Attempts].StringValue).IsEqualTo("32");
            using var history = JsonDocument.Parse(copy.MessageAttributes[FailureHeaders.History].StringValue);
            await Assert.That(history.RootElement.GetProperty("Truncated").GetBoolean()).IsTrue();
            var attempts = history.RootElement.GetProperty("Attempts");
            await Assert.That(attempts[0].GetProperty("Attempt").GetInt32()).IsEqualTo(1);
            await Assert.That(attempts[attempts.GetArrayLength() - 1].GetProperty("Attempt").GetInt32()).IsEqualTo(32);
            var size = Encoding.UTF8.GetByteCount(copy.Body);
            foreach (var attribute in copy.MessageAttributes)
                size += Encoding.UTF8.GetByteCount(attribute.Key) + Encoding.UTF8.GetByteCount(attribute.Value.DataType) + Encoding.UTF8.GetByteCount(attribute.Value.StringValue);
            await Assert.That(size <= 4096).IsTrue();
            await host.StopAsync(token);
            var sourceUrl = (await client.GetQueueUrlAsync(source, token)).QueueUrl;
            var remaining = await client.GetQueueAttributesAsync(sourceUrl, ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"], token);
            await Assert.That(remaining.Attributes["ApproximateNumberOfMessages"]).IsEqualTo("0");
            await Assert.That(remaining.Attributes["ApproximateNumberOfMessagesNotVisible"]).IsEqualTo("0");
        }
        finally { await host.StopAsync(CancellationToken.None); }
    }

    [Message("failure.history.sqs")]
    public sealed class Payload;
    public sealed class Handler : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => throw new Poison();
    }
    private sealed class Poison : Exception
    {
        public override string Message => "poison " + new string('界', 10000);
        public override string StackTrace => new('x', 200000);
    }
}
