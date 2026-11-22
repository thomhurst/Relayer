using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.AmazonSqs;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class SqsFailureTests
{
    [Test]
    [Arguments("ce_id", "ce_id", false)]
    [Arguments("ce_id", "custom_id", true)]
    [Arguments("missing_id", "ce_id", true)]
    public async Task Fifo_copy_uses_native_fallback_only_when_a_bound_identity_is_missing(string sourceHeader, string deadHeader, bool needsFallback)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var delivery = new SqsDelivery(client, "url.fifo");
        var message = SqsReceiveTests.Message("body", 1);
        message.Attributes["MessageDeduplicationId"] = "native-id";
        message.Attributes["MessageGroupId"] = "group";
        delivery.Prepare(message, TimeProvider.System.GetTimestamp(), null);
        var context = new CopyContext();
        context.Features.Set(new NativeMessage<SqsDelivery> { Message = delivery });
        var copy = SqsHostedService.Copy(context, sourceHeader, deadHeader);
        var state = new SqsSendState("0");
        state.Prepare(copy, "dead.fifo", true, deadHeader);
        await Assert.That(state.Request.MessageDeduplicationId).IsEqualTo(needsFallback ? "native-id" : "id");
        await Assert.That(state.Request.MessageAttributes["ce_id"].DataType).IsEqualTo("String");
        await Assert.That(state.Request.MessageAttributes["ce_id"].StringValue).IsEqualTo("id");

        if (needsFallback)
        {
            await Assert.That(() => copy.ValidateForOutbox()).Throws<InvalidOperationException>();
        }
        else
        {
            copy.ValidateForOutbox();
        }
    }

    [Test]
    [Arguments("Number", "ce_id", "ce_id")]
    [Arguments("String.eventId", "ce_id", "ce_id")]
    [Arguments("Number", "source_id", "ce_id")]
    [Arguments("String.eventId", "ce_id", "dead_id")]
    public async Task Fifo_copy_preserves_native_deduplication_for_typed_identity_attributes(string dataType, string sourceHeader, string deadHeader)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var delivery = new SqsDelivery(client, "url.fifo");
        var message = SqsReceiveTests.Message("body", 1);
        message.MessageAttributes[sourceHeader] = new()
        {
            DataType = dataType,
            StringValue = "42"
        };
        message.MessageAttributes[deadHeader] = new()
        {
            DataType = dataType,
            StringValue = "42"
        };
        message.Attributes["MessageDeduplicationId"] = "native-id";
        message.Attributes["MessageGroupId"] = "group";
        delivery.Prepare(message, TimeProvider.System.GetTimestamp(), null);
        var context = new CopyContext();
        context.Features.Set(new NativeMessage<SqsDelivery> { Message = delivery });
        var copy = SqsHostedService.Copy(context, sourceHeader, deadHeader);
        var state = new SqsSendState("0");
        state.Prepare(copy, "dead.fifo", true, deadHeader);
        await Assert.That(state.Request.MessageDeduplicationId).IsEqualTo("native-id");
        await Assert.That(state.Request.MessageAttributes[deadHeader].DataType).IsEqualTo(dataType);
        await Assert.That(state.Request.MessageAttributes[deadHeader].StringValue).IsEqualTo("42");
        await Assert.That(() => copy.ValidateForOutbox()).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(" ", "ce_id")]
    [Arguments("\t", "ce_id")]
    [Arguments("event 1", "ce_id")]
    [Arguments("évent", "custom_id")]
    [Arguments("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "ce_id")]
    [Arguments(" ", "custom_id")]
    public async Task Fifo_copy_preserves_native_fallback_and_unusable_identity(string identity, string identityHeader)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var delivery = new SqsDelivery(client, "url.fifo");
        var message = SqsReceiveTests.Message("body", 1);
        message.MessageAttributes[identityHeader] = new()
        {
            DataType = "String",
            StringValue = identity
        };
        message.Attributes["MessageDeduplicationId"] = "native-id";
        message.Attributes["MessageGroupId"] = "group";
        delivery.Prepare(message, TimeProvider.System.GetTimestamp(), null);
        var context = new CopyContext();
        context.Features.Set(new NativeMessage<SqsDelivery> { Message = delivery });
        var copy = SqsHostedService.Copy(context, identityHeader, identityHeader);
        var state = new SqsSendState("0");
        state.Prepare(copy, "dead.fifo", true, identityHeader);
        await Assert.That(state.Request.MessageDeduplicationId).IsEqualTo("native-id");
        await Assert.That(state.Request.MessageAttributes[identityHeader].DataType).IsEqualTo("String");
        await Assert.That(state.Request.MessageAttributes[identityHeader].StringValue).IsEqualTo(identity);
        await Assert.That(() => copy.ValidateForOutbox()).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(1)]
    [Arguments(128)]
    public async Task Fifo_copy_keeps_valid_deduplication_boundaries_persistable(int length)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var delivery = new SqsDelivery(client, "url.fifo");
        var identity = new string('~', length);
        var message = SqsReceiveTests.Message("body", 1);
        message.MessageAttributes["ce_id"].StringValue = identity;
        message.Attributes["MessageDeduplicationId"] = "native-id";
        message.Attributes["MessageGroupId"] = "group";
        delivery.Prepare(message, TimeProvider.System.GetTimestamp(), null);
        var context = new CopyContext();
        context.Features.Set(new NativeMessage<SqsDelivery> { Message = delivery });
        var copy = SqsHostedService.Copy(context);
        var state = new SqsSendState("0");
        state.Prepare(copy, "dead.fifo", true);
        await Assert.That(state.Request.MessageDeduplicationId).IsEqualTo(identity);
        copy.ValidateForOutbox();
    }

    [Test]
    public async Task Standard_fair_queue_copy_preserves_native_group_without_portable_ordering()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var delivery = new SqsDelivery(client, "https://sqs.test/123/orders");
        var message = SqsReceiveTests.Message("body", 1);
        message.Attributes["MessageGroupId"] = "tenant";
        delivery.Prepare(message, TimeProvider.System.GetTimestamp(), null);
        var context = new CopyContext();
        context.Features.Set(new NativeMessage<SqsDelivery> { Message = delivery });
        var copy = SqsHostedService.Copy(context);
        await Assert.That(copy.OrderingKey).IsNull();
        var state = new SqsSendState("0");
        state.Prepare(copy, "dead", false);
        await Assert.That(state.Request.MessageGroupId).IsEqualTo("tenant");
        await Assert.That(() => copy.ValidateForOutbox()).Throws<InvalidOperationException>();
    }

    private sealed class CopyContext : MessageContext
    {
        public override string MessageId => "id";

        public override MessageType MessageType => default;

        public override MessageHeaders Headers => MessageHeaders.Empty;

        public override int Attempt => 1;

        public override IServiceProvider Services => throw new NotSupportedException();

        public override MessageFeatures Features
        {
            get;
        } = new();

        public override CancellationToken CancellationToken => default;
    }

    [Test]
    public async Task Failed_poison_copy_never_deletes_the_original()
    {
        using var client = new SqsReceiveTests.ReceiveClient { SendFailure = new IOException("DLQ unavailable") };
        using var host = Host(client);
        await host.StartAsync();
        await client.Deliveries.Writer.WriteAsync(SqsReceiveTests.Message("{\"Value\":1}", 1));
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(0);
        await host.StopAsync();
    }

    [Test]
    public async Task Native_attribute_capacity_failure_retains_original_instead_of_dropping_metadata()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client);
        await host.StartAsync();
        var message = SqsReceiveTests.Message("{\"Value\":1}", 1);

        for (var index = 0; index < 6; index++)
        {
            message.MessageAttributes.Add("application-" + index, new()
            {
                DataType = "String",
                StringValue = "value"
            });
        }

        await client.Deliveries.Writer.WriteAsync(message);
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(0);
        await Assert.That(message.MessageAttributes.Count).IsEqualTo(8);
        await Assert.That(client.Sends).IsEqualTo(0);
        await host.StopAsync();
    }

    [Test]
    public async Task Invalid_native_receive_count_reaches_poison_copy_instead_of_failing_before_pipeline()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client);
        await host.StartAsync();
        var message = SqsReceiveTests.Message("{\"Value\":1}", 1);
        message.Attributes["ApproximateReceiveCount"] = "invalid";
        await client.Deliveries.Writer.WriteAsync(message);
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(1);
        await Assert.That(client.Sends).IsEqualTo(1);
        await host.StopAsync();
    }

    [Test]
    public async Task Largest_whole_second_below_twelve_hours_is_accepted_without_a_scheduler()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client, TimeSpan.FromSeconds(43_199));
        await host.StartAsync();
        await host.StopAsync();
    }

    [Test]
    [Arguments(43_199 * TimeSpan.TicksPerSecond + 1)]
    [Arguments(43_200 * TimeSpan.TicksPerSecond)]
    public async Task Delay_that_rounds_to_twelve_hours_requires_a_scheduler_at_startup(long delayTicks)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client, TimeSpan.FromTicks(delayTicks));
        await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>();
        await Assert.That(client.Polls).IsEqualTo(0);
    }

    [Test]
    [Arguments(43_199 * TimeSpan.TicksPerSecond + 1)]
    [Arguments(43_200 * TimeSpan.TicksPerSecond)]
    [Arguments(46_800 * TimeSpan.TicksPerSecond)]
    public async Task Unsupported_native_delay_waits_for_durable_outbox_before_deletion(long delayTicks)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        var scheduler = new Scheduler();
        var delay = TimeSpan.FromTicks(delayTicks);
        using var host = Host(client, delay, scheduler);
        await host.StartAsync();
        var before = DateTimeOffset.UtcNow;
        await client.Deliveries.Writer.WriteAsync(SqsReceiveTests.Message("{\"Value\":1}", 1));
        var first = await Task.WhenAny(scheduler.Started.Task, client.NextPoll.Task).WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(first).IsEqualTo(scheduler.Started.Task);
        await Assert.That(client.Deletes).IsEqualTo(0);
        await Assert.That(client.Visibility.IsEmpty).IsTrue();
        await Assert.That(scheduler.Transport).IsEqualTo("sqs");
        await Assert.That(scheduler.Destination).IsEqualTo("orders");
        await Assert.That(scheduler.Due >= before + delay && scheduler.Due <= DateTimeOffset.UtcNow + delay).IsTrue();
        await Assert.That(scheduler.Body).IsEqualTo("{\"Value\":1}");
        scheduler.Commit.SetResult();
        await client.NextPoll.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(client.Deletes).IsEqualTo(1);
        await host.StopAsync();
    }

    [Test]
    [Arguments(null)]
    [Arguments("ce_")]
    [Arguments("retry_")]
    public async Task Durable_redelivery_uses_the_source_binding_and_deletes_after_scheduling(string? fifoSourcePrefix)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
        {
            ServiceURL = await SharedBrokers.FlociUrlAsync(token),
            AuthenticationRegion = "us-east-1"
        });
        var fifo = fifoSourcePrefix is not null;
        var suffix = fifo ? ".fifo" : "";
        var source = TestContext.Current!.Isolation.GetIsolatedName("source") + suffix;
        var dead = TestContext.Current!.Isolation.GetIsolatedName("dead") + suffix;
        var sourceUrl = (await client.CreateQueueAsync(Queue(source), token)).QueueUrl;
        await client.CreateQueueAsync(Queue(dead), token);
        var deleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        client.AfterResponseEvent += (_, args) =>
        {
            if (args is WebServiceResponseEventArgs { Response: DeleteMessageResponse })
            {
                deleted.TrySetResult();
            }
        };

        var scheduler = new Scheduler();
        var delay = TimeSpan.FromHours(fifo ? 13 : 12);
        using var host = Host(client, delay, scheduler, source, dead, fifoSourcePrefix);

        try
        {
            await host.StartAsync(token);
            var message = SqsReceiveTests.Message("{\"Value\":1}", 1);
            var identityHeader = new HeaderNames(fifoSourcePrefix ?? "ce_").Id;

            if (identityHeader != "ce_id")
            {
                // The receive binding can decode the envelope while a separate send binding
                // supplies the destination's native FIFO deduplication ID.
                message.MessageAttributes.Add(identityHeader, new()
                {
                    DataType = "String",
                    StringValue = "retry-id"
                });
            }

            await client.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = sourceUrl,
                MessageBody = message.Body,
                MessageAttributes = message.MessageAttributes,
                MessageGroupId = fifo ? "group" : null,
                MessageDeduplicationId = fifo ? "native-id" : null
            }, token);
            await scheduler.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), token);
            await Assert.That(scheduler.Destination).IsEqualTo(source);
            await Assert.That(scheduler.Body).IsEqualTo(message.Body);
            await Assert.That(scheduler.Headers["ce_id"]).IsEqualTo("id");
            await Assert.That(scheduler.Headers[identityHeader]).IsEqualTo(message.MessageAttributes[identityHeader].StringValue);
            await Assert.That(scheduler.Headers[FailureHeaders.Redeliveries]).IsEqualTo("1");
            await Assert.That(scheduler.OrderingKey).IsEqualTo(fifo ? "group" : null);
            await Assert.That(deleted.Task.IsCompleted).IsFalse();

            scheduler.Commit.SetResult();
            await deleted.Task.WaitAsync(TimeSpan.FromSeconds(10), token);
        }
        finally
        {
            await host.StopAsync(token);
        }
    }

    private static CreateQueueRequest Queue(string name)
    {
        return new(name)
        {
            Attributes = name.EndsWith(".fifo", StringComparison.Ordinal) ? new()
            {
                ["FifoQueue"] = "true"
            } : []
        };
    }

    private static IHost Host(IAmazonSQS client, TimeSpan? redeliveryDelay = null, Scheduler? scheduler = null,
        string source = "orders", string dead = "dead", string? fifoSourcePrefix = null)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new SqsReceiveFlociTests.Control());

        if (scheduler is not null)
        {
            builder.Services.AddSingleton<IFailureScheduler>(scheduler);
        }

        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AmazonSqs("sqs", sqs =>
            {
                sqs.TopologyMode = TopologyMode.None;
                sqs.Options.Configure(options =>
                {
                    options.SqsClient = client;
                    options.Queues.Add(Queue(source));
                    options.Queues.Add(Queue(dead));
                });

                if (fifoSourcePrefix is not null)
                {
                    sqs.Send<SqsSendTests.Order>(dead, route =>
                    {
                        route.SendOptions = new()
                        {
                            HeaderNames = new("dead_")
                        };
                        route.OrderingKey(static _ => "group");
                    });

                    if (fifoSourcePrefix != "ce_")
                    {
                        sqs.Send<SqsReceiveFlociTests.Payload>(source, route =>
                        {
                            route.SendOptions = new()
                            {
                                HeaderNames = new(fifoSourcePrefix)
                            };
                            route.OrderingKey(static _ => "group");
                        });
                    }
                }

                sqs.Consume(source, consumer =>
                {
                    consumer.Handler<SqsReceiveFlociTests.Handler>();
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = dead,
                        RetryDelays = [],
                        Jitter = 0,
                        RedeliveryDelays = redeliveryDelay is { } delay ? [delay] : []
                    });
                });
            });
        });
        return builder.Build();
    }

    private sealed class Scheduler : IFailureScheduler
    {
        internal readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Commit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal string? Transport;
        internal string? Destination;
        internal string? Body;
        internal string? OrderingKey;
        internal readonly Dictionary<string, string> Headers = [];
        internal DateTimeOffset? Due;

        public async ValueTask ScheduleAsync(string transportName, OutgoingMessage message, CancellationToken token)
        {
            Transport = transportName;
            Destination = message.Destination;
            Due = message.DeliverAt;
            Body = System.Text.Encoding.UTF8.GetString(message.Body.Span);
            OrderingKey = message.OrderingKey?.ToString();

            foreach (var header in message.Headers.Span)
            {
                Headers.Add(header.Key, header.Value.ToString());
            }

            Started.SetResult();
            await Commit.Task.WaitAsync(token);
        }
    }
}
