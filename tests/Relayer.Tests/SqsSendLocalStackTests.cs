using Relayer.Transports;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using Relayer.AmazonSqs;

namespace Relayer.Tests;

public class SqsSendLocalStackTests
{
    [Test]
    public async Task Standard_fifo_batches_delays_and_read_only_topology_round_trip()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var externalUrl = Environment.GetEnvironmentVariable("RELAYER_SQS_SERVICE_URL");
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
        {
            ServiceURL = externalUrl ?? await SharedBrokers.LocalStackUrlAsync(token),
            AuthenticationRegion = "us-east-1",
        });
        var standard = "relayer-" + Guid.NewGuid().ToString("N");
        var fifo = standard + ".fifo";
        var options = new AmazonSqsTransportOptions { SqsClient = client };
        options.Queues.Add(new(standard) { Attributes = new() { ["MaximumMessageSize"] = "4096" }, Tags = new() { ["application"] = "relayer" } });
        options.Queues.Add(new(fifo) { Attributes = new() { ["FifoQueue"] = "true" } });
        var urls = new List<string>();
        try
        {
            IRouteDefinition[] routes = [new RouteDefinition<SqsSendTests.Order>(fifo, null, null, new() { HeaderNames = new("event_") })];
            await using var transport = new SqsSendTransport("localstack", client, false, options, identityHeaders: SqsSendTransport.ResolveIdentityHeaders(routes));
            var missing = await transport.ApplyTopologyAsync(TopologyMode.Validate, token);
            await Assert.That(missing.Differences.Count).IsEqualTo(2);
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
            var standardUrl = (await client.GetQueueUrlAsync(standard, token)).QueueUrl;
            var fifoUrl = (await client.GetQueueUrlAsync(fifo, token)).QueueUrl;
            urls.Add(standardUrl);
            urls.Add(fifoUrl);
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
            await Assert.That(transport.GetCapabilities(standard).MaxMessageBytes).IsEqualTo(4096);

            KeyValuePair<string, WireString>[] headers = [new("ce_id", new("event-id")), new("ce_type", new("orders.created;v=1")), new("content-type", new("application/json")), new("traceparent", new("creation-context"))];
            await transport.SendAsync(new(standard, "{\"value\":1}"u8.ToArray(), headers), token);
            var received = await ReceiveAsync(client, standardUrl, token);
            await Assert.That(received.Body).IsEqualTo("{\"value\":1}");
            await Assert.That(received.MessageAttributes["ce_type"].StringValue).IsEqualTo("orders.created;v=1");
            await Assert.That(received.MessageAttributes["content-type"].StringValue).IsEqualTo("application/json");
            await Assert.That(received.MessageAttributes["traceparent"].StringValue).IsEqualTo("creation-context");
            await client.DeleteMessageAsync(standardUrl, received.ReceiptHandle, token);

            var batch = new OutgoingMessage[12];
            for (var i = 0; i < batch.Length; i++)
                batch[i] = new(fifo, "identical body"u8.ToArray(),
                    new KeyValuePair<string, WireString>[] { new("event_id", new WireString("event-" + i)) }, new WireString("customer"));
            await transport.SendBatchAsync(batch, token);
            for (var i = 0; i < batch.Length; i++)
            {
                var ordered = await ReceiveAsync(client, fifoUrl, token);
                await Assert.That(ordered.Body).IsEqualTo("identical body");
                await Assert.That(ordered.MessageAttributes["event_id"].StringValue).IsEqualTo("event-" + i);
                await Assert.That(ordered.Attributes["MessageGroupId"]).IsEqualTo("customer");
                await Assert.That(ordered.Attributes["MessageDeduplicationId"]).IsEqualTo("event-" + i);
                await client.DeleteMessageAsync(fifoUrl, ordered.ReceiptHandle, token);
            }
            // Replaying the stable event identity uses native FIFO deduplication, without a second delivery.
            await transport.SendAsync(batch[0], token);
            var duplicate = await client.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = fifoUrl, WaitTimeSeconds = 1 }, token);
            await Assert.That(duplicate.Messages?.Count ?? 0).IsEqualTo(0);

            var due = DateTimeOffset.UtcNow.AddSeconds(2);
            await transport.SendAsync(new(standard, "scheduled"u8.ToArray(), deliverAt: due), token);
            var early = await client.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = standardUrl, WaitTimeSeconds = 0 }, token);
            await Assert.That(early.Messages?.Count ?? 0).IsEqualTo(0);
            var delayed = await ReceiveAsync(client, standardUrl, token);
            await Assert.That(DateTimeOffset.UtcNow >= due).IsTrue();
            await Assert.That(delayed.Body).IsEqualTo("scheduled");
            await client.DeleteMessageAsync(standardUrl, delayed.ReceiptHandle, token);

            options.Queues[0].Attributes["MaximumMessageSize"] = "8192";
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Single()).Contains("MaximumMessageSize");
            var unchanged = await client.GetQueueAttributesAsync(standardUrl, ["MaximumMessageSize"], token);
            await Assert.That(unchanged.Attributes["MaximumMessageSize"]).IsEqualTo("4096");
            options.Queues[0].Attributes["MaximumMessageSize"] = "4096";
            options.Queues[0].Tags["application"] = "changed";
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Single()).Contains("tag 'application'");
            var tags = await client.ListQueueTagsAsync(new ListQueueTagsRequest { QueueUrl = standardUrl }, token);
            await Assert.That(tags.Tags["application"]).IsEqualTo("relayer");
        }
        finally
        {
            foreach (var url in urls) await client.DeleteQueueAsync(url, CancellationToken.None);
        }
    }

    private static async Task<Message> ReceiveAsync(IAmazonSQS client, string url, CancellationToken token)
    {
        var response = await client.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = url, WaitTimeSeconds = 10, MaxNumberOfMessages = 1,
            MessageAttributeNames = ["All"], MessageSystemAttributeNames = ["All"],
        }, token);
        return response.Messages?.SingleOrDefault() ?? throw new InvalidOperationException("LocalStack did not deliver the expected SQS message.");
    }
}
