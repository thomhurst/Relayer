using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using Relayer.AmazonSqs;

namespace Relayer.Tests;

public class SnsSendLocalStackTests
{
    [Test]
    public async Task Topics_raw_subscriptions_queue_policies_fifo_and_read_only_topology_round_trip()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        var externalUrl = Environment.GetEnvironmentVariable("RELAYER_SNS_SERVICE_URL");
        var url = externalUrl ?? await SharedBrokers.LocalStackUrlAsync(token);
        using var sns = new AmazonSimpleNotificationServiceClient(new BasicAWSCredentials("test", "test"), new AmazonSimpleNotificationServiceConfig { ServiceURL = url, AuthenticationRegion = "us-east-1" });
        using var sqs = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = url, AuthenticationRegion = "us-east-1" });
        var name = "relayer-" + Guid.NewGuid().ToString("N");
        var options = new AmazonSnsTransportOptions { SnsClient = sns };
        options.QueueTopology.SqsClient = sqs;
        foreach (var suffix in new[] { "", ".fifo" })
        {
            var destination = name + suffix;
            var topicArn = "arn:aws:sns:us-east-1:000000000000:" + destination;
            var queueArn = "arn:aws:sqs:us-east-1:000000000000:" + destination;
            options.Topics.Add(new(destination) { Attributes = suffix.Length == 0 ? null : new() { ["FifoTopic"] = "true" }, Tags = [new() { Key = "application", Value = "relayer" }] });
            var attributes = new Dictionary<string, string>
            {
                ["Policy"] = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"sns.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"" + queueArn + "\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"" + topicArn + "\"}}}]}"
            };
            if (suffix.Length != 0) attributes.Add("FifoQueue", "true");
            options.QueueTopology.Queues.Add(new(destination) { Attributes = attributes });
            options.Subscriptions.Add(new() { TopicArn = topicArn, Protocol = "sqs", Endpoint = queueArn, ReturnSubscriptionArn = true, Attributes = new() { ["RawMessageDelivery"] = "true" } });
        }
        await using var transport = new SnsSendTransport("localstack", sns, false, options);
        try
        {
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(6);
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
            await transport.StartAsync(token);
            KeyValuePair<string, WireString>[] headers = [new("ce_id", new("event-1")), new("ce_type", new("orders.created;v=1")), new("content-type", new("application/json")), new("traceparent", new("creation-context"))];
            await transport.SendAsync(new(name, "{\"value\":1}"u8.ToArray(), headers), token);
            var queueUrl = (await sqs.GetQueueUrlAsync(name, token)).QueueUrl;
            var received = await ReceiveAsync(sqs, queueUrl, token);
            await Assert.That(received.Body).IsEqualTo("{\"value\":1}");
            await Assert.That(received.MessageAttributes["ce_type"].StringValue).IsEqualTo("orders.created;v=1");
            await Assert.That(received.MessageAttributes["content-type"].StringValue).IsEqualTo("application/json");
            await Assert.That(received.MessageAttributes["traceparent"].StringValue).IsEqualTo("creation-context");
            await sqs.DeleteMessageAsync(queueUrl, received.ReceiptHandle, token);
            var batch = new OutgoingMessage[12];
            for (var i = 0; i < batch.Length; i++) batch[i] = new(name + ".fifo", "identical"u8.ToArray(), new KeyValuePair<string, WireString>[] { new("ce_id", new WireString("event-" + i)) }, new WireString("group"));
            await transport.SendBatchAsync(batch, token);
            var fifoUrl = (await sqs.GetQueueUrlAsync(name + ".fifo", token)).QueueUrl;
            for (var i = 0; i < batch.Length; i++)
            {
                var ordered = await ReceiveAsync(sqs, fifoUrl, token);
                await Assert.That(ordered.MessageAttributes["ce_id"].StringValue).IsEqualTo("event-" + i);
                await Assert.That(ordered.Attributes["MessageGroupId"]).IsEqualTo("group");
                await sqs.DeleteMessageAsync(fifoUrl, ordered.ReceiptHandle, token);
            }
            await transport.SendAsync(batch[0], token);
            await Assert.That((await sqs.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = fifoUrl, WaitTimeSeconds = 1 }, token)).Messages?.Count ?? 0).IsEqualTo(0);
            options.Subscriptions[0].Attributes["RawMessageDelivery"] = "false";
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Single()).Contains("RawMessageDelivery");
            var subscription = (await sns.ListSubscriptionsByTopicAsync(options.Subscriptions[0].TopicArn, token)).Subscriptions.Single();
            await Assert.That((await sns.GetSubscriptionAttributesAsync(subscription.SubscriptionArn, token)).Attributes["RawMessageDelivery"]).IsEqualTo("true");
            options.Topics[0].Tags[0].Value = "changed";
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Any(difference => difference.Contains("tag 'application'"))).IsTrue();
        }
        finally
        {
            foreach (var topic in options.Subscriptions)
            {
                try { await sns.DeleteTopicAsync(topic.TopicArn, CancellationToken.None); }
                catch (NotFoundException) { }
            }
            foreach (var queue in options.QueueTopology.Queues)
            {
                try { await sqs.DeleteQueueAsync((await sqs.GetQueueUrlAsync(queue.QueueName, CancellationToken.None)).QueueUrl, CancellationToken.None); }
                catch (QueueDoesNotExistException) { }
            }
        }
    }
    private static async Task<Amazon.SQS.Model.Message> ReceiveAsync(IAmazonSQS sqs, string url, CancellationToken token)
    {
        var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest { QueueUrl = url, WaitTimeSeconds = 10, MaxNumberOfMessages = 1, MessageAttributeNames = ["All"], MessageSystemAttributeNames = ["All"] }, token);
        return response.Messages?.SingleOrDefault() ?? throw new InvalidOperationException("LocalStack did not deliver the SNS notification to SQS.");
    }
}
