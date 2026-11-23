using System.Diagnostics.Metrics;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Options;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public class AmazonSendCancellationFlociTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Sqs_cancellation_during_request_preparation_emits_no_native_metrics(bool batch)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
        {
            ServiceURL = Environment.GetEnvironmentVariable("RELAYER_SQS_SERVICE_URL") ?? await SharedBrokers.FlociUrlAsync(token),
            AuthenticationRegion = "us-east-1",
        });
        var queue = TestContext.Current!.Isolation.GetIsolatedName("sqs-cancel");
        var declaration = new CreateQueueRequest(queue);
        var url = (await client.CreateQueueAsync(declaration, token)).QueueUrl;

        try
        {
            using var instrumentation = new RelayerInstrumentation();
            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
            var options = new AmazonSqsTransportOptions { SqsClient = client };
            options.Queues.Add(declaration);
            await using var transport = new SqsSendTransport("floci", client, false, options, tracing);
            using var cancelled = new CancellationTokenSource();
            var send = new SendOptions().AmazonSqs(_ => cancelled.Cancel());
            await transport.StartAsync(token);
            await AssertCancellationMetricsAsync(transport, queue, batch, send, cancelled.Token, instrumentation, token);
        }
        finally
        {
            await client.DeleteQueueAsync(url, CancellationToken.None);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Sns_cancellation_during_request_preparation_emits_no_native_metrics(bool batch)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        using var client = new AmazonSimpleNotificationServiceClient(new BasicAWSCredentials("test", "test"), new AmazonSimpleNotificationServiceConfig
        {
            ServiceURL = Environment.GetEnvironmentVariable("RELAYER_SNS_SERVICE_URL") ?? await SharedBrokers.FlociUrlAsync(token),
            AuthenticationRegion = "us-east-1",
        });
        var topic = TestContext.Current!.Isolation.GetIsolatedName("sns-cancel");
        var arn = (await client.CreateTopicAsync(topic, token)).TopicArn;

        try
        {
            using var instrumentation = new RelayerInstrumentation();
            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
            var options = new AmazonSnsTransportOptions { SnsClient = client };
            options.Topics.Add(new(topic));
            await using var transport = new SnsSendTransport("floci", client, false, options, tracing);
            using var cancelled = new CancellationTokenSource();
            var send = new SendOptions().AmazonSns(_ => cancelled.Cancel());
            await transport.StartAsync(token);
            await AssertCancellationMetricsAsync(transport, topic, batch, send, cancelled.Token, instrumentation, token);
        }
        finally
        {
            await client.DeleteTopicAsync(arn, CancellationToken.None);
        }
    }

    private static async Task AssertCancellationMetricsAsync(ITransport transport, string destination, bool batch, SendOptions send,
        CancellationToken cancelled, RelayerInstrumentation instrumentation, CancellationToken token)
    {
        using var durations = new SendDurationCapture(instrumentation);
        long sent = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meter) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.sent.messages")
                {
                    meter.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, count, _, _) => Interlocked.Add(ref sent, count));
        listener.Start();

        var message = new OutgoingMessage(destination, "value"u8.ToArray(), options: send);
        await Assert.That(async () => await SendAsync(message, cancelled)).Throws<OperationCanceledException>();
        await Assert.That(sent).IsEqualTo(0);
        await Assert.That(durations.Values).IsEmpty();

        await SendAsync(new(destination, "value"u8.ToArray()), token);
        await Assert.That(sent).IsEqualTo(1);
        await Assert.That(durations.Values.Single().Tags.ContainsKey("error.type")).IsFalse();

        ValueTask SendAsync(OutgoingMessage outgoing, CancellationToken cancellationToken)
        {
            return batch ? transport.SendBatchAsync(new[] { outgoing }, cancellationToken) : transport.SendAsync(outgoing, cancellationToken);
        }
    }
}
