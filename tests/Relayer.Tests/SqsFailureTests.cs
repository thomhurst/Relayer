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
    [Arguments(0, 0, 0, 3, false)]
    [Arguments(0, 0, 0, 4, true)]
    [Arguments(0, 46_800, 0, 2, true)]
    [Arguments(46_800, 0, 0, 2, false)]
    [Arguments(46_800, 0, 0, 3, true)]
    [Arguments(46_800, 50_400, 46_800, 1, true)]
    public async Task Redrive_budget_covers_the_longest_run_of_visibility_retries(
        int firstSeconds, int secondSeconds, int thirdSeconds, int maxReceiveCount, bool starts)
    {
        using var client = new SqsReceiveTests.ReceiveClient
        {
            Redrive = $$"""{"maxReceiveCount":{{maxReceiveCount}},"deadLetterTargetArn":"arn"}"""
        };
        using var host = Host(client,
            [TimeSpan.FromSeconds(firstSeconds), TimeSpan.FromSeconds(secondSeconds), TimeSpan.FromSeconds(thirdSeconds)],
            new Scheduler());

        if (starts)
        {
            await host.StartAsync();
            await host.StopAsync();
        }
        else
        {
            var thrown = await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>();
            await Assert.That(thrown!.Message).Contains("RedrivePolicy.maxReceiveCount");
            await Assert.That(client.Polls).IsEqualTo(0);
        }
    }

    [Test]
    public async Task Largest_whole_second_below_twelve_hours_is_accepted_without_a_scheduler()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client, [TimeSpan.FromSeconds(43_199)]);
        await host.StartAsync();
        await host.StopAsync();
    }

    [Test]
    [Arguments(43_199 * TimeSpan.TicksPerSecond + 1)]
    [Arguments(43_200 * TimeSpan.TicksPerSecond)]
    public async Task Delay_that_rounds_to_twelve_hours_requires_a_scheduler_at_startup(long delayTicks)
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        using var host = Host(client, [TimeSpan.FromTicks(delayTicks)]);
        await Assert.That(() => host.StartAsync()).Throws<InvalidOperationException>();
        await Assert.That(client.Polls).IsEqualTo(0);
    }

    [Test]
    [Arguments(43_199 * TimeSpan.TicksPerSecond + 1)]
    [Arguments(43_200 * TimeSpan.TicksPerSecond)]
    [Arguments(46_800 * TimeSpan.TicksPerSecond)]
    public async Task Unsupported_native_delay_waits_for_durable_outbox_before_deletion(long delayTicks)
    {
        using var client = new SqsReceiveTests.ReceiveClient
        {
            Redrive = """{"maxReceiveCount":1,"deadLetterTargetArn":"arn"}"""
        };
        var scheduler = new Scheduler();
        var delay = TimeSpan.FromTicks(delayTicks);
        using var host = Host(client, [delay], scheduler);
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
    [Arguments(null, null)]
    [Arguments("ce_", null)]
    [Arguments("ce_", "retry_")]
    [Arguments("event_", null)]
    [Arguments("event_", "event_")]
    public async Task Durable_redelivery_uses_the_source_binding_and_deletes_after_scheduling(
        string? receivePrefix, string? sendPrefix)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        using var client = new AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig
        {
            ServiceURL = await SharedBrokers.FlociUrlAsync(token),
            AuthenticationRegion = "us-east-1"
        });
        var fifo = receivePrefix is not null;
        var suffix = fifo ? ".fifo" : "";
        var source = TestContext.Current!.Isolation.GetIsolatedName("source") + suffix;
        var dead = TestContext.Current!.Isolation.GetIsolatedName("dead") + suffix;
        var deadUrl = (await client.CreateQueueAsync(Queue(dead), token)).QueueUrl;
        var deadArn = (await client.GetQueueAttributesAsync(deadUrl, ["QueueArn"], token)).QueueARN;
        var sourceQueue = Queue(source);
        sourceQueue.Attributes["RedrivePolicy"] = $$"""{"maxReceiveCount":"2","deadLetterTargetArn":"{{deadArn}}"}""";
        var sourceUrl = (await client.CreateQueueAsync(sourceQueue, token)).QueueUrl;
        var redrive = await client.GetQueueAttributesAsync(sourceUrl, ["RedrivePolicy"], token);
        await Assert.That(redrive.Attributes["RedrivePolicy"]).Contains(deadArn);

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
        using var host = Host(client, [delay, TimeSpan.FromHours(14)], scheduler, source, dead, receivePrefix, sendPrefix);

        try
        {
            await host.StartAsync(token);
            var message = SqsReceiveTests.Message("{\"Value\":1}", 1);
            var receiveNames = new HeaderNames(receivePrefix ?? "ce_");
            var id = message.MessageAttributes["ce_id"];
            var type = message.MessageAttributes["ce_type"];
            message.MessageAttributes = new()
            {
                [receiveNames.Id] = id,
                [receiveNames.Type] = type
            };
            var identityHeader = sendPrefix is null ? receiveNames.Id : new HeaderNames(sendPrefix).Id;

            if (identityHeader != receiveNames.Id)
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
            await Assert.That(scheduler.Headers[receiveNames.Id]).IsEqualTo("id");
            await Assert.That(scheduler.Headers[receiveNames.Type]).IsEqualTo("sqs.receive;v=1");
            await Assert.That(scheduler.Headers.ContainsKey("ce_id")).IsEqualTo(receiveNames.Id == "ce_id");
            await Assert.That(scheduler.Headers[identityHeader]).IsEqualTo(message.MessageAttributes[identityHeader].StringValue);
            await Assert.That(scheduler.Headers[FailureHeaders.Redeliveries]).IsEqualTo("1");
            await Assert.That(scheduler.OrderingKey).IsEqualTo(fifo ? "group" : null);
            await Assert.That(deleted.Task.IsCompleted).IsFalse();

            // Drain the active delivery without leaving a broker long poll pending during replay.
            var stopping = host.StopAsync(token);
            scheduler.Commit.SetResult();
            await deleted.Task.WaitAsync(TimeSpan.FromSeconds(10), token);
            await stopping;

            if (fifo)
            {
                // Rebuild the send from stored fields, as an outbox dispatcher does when it is due.
                var transport = host.Services.GetRequiredKeyedService<SqsSendTransport>("sqs");
                var headers = scheduler.Headers.Select(static header =>
                    new KeyValuePair<string, WireString>(header.Key, new(header.Value))).ToArray();
                await transport.SendAsync(new(scheduler.Destination!, System.Text.Encoding.UTF8.GetBytes(scheduler.Body!),
                    headers, new WireString(scheduler.OrderingKey!)), token);

                var replay = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = sourceUrl,
                    WaitTimeSeconds = 10,
                    MessageAttributeNames = ["All"],
                    MessageSystemAttributeNames = ["All"]
                }, token);
                var retried = replay.Messages.Single();
                await Assert.That(retried.Attributes["MessageDeduplicationId"]).IsEqualTo(scheduler.Headers[identityHeader]);
                await Assert.That(retried.Attributes["MessageGroupId"]).IsEqualTo("group");
                await Assert.That(retried.MessageAttributes[receiveNames.Id].StringValue).IsEqualTo("id");
                await Assert.That(retried.Body).IsEqualTo(message.Body);
            }
        }
        finally
        {
            await host.StopAsync(token);
        }
    }

    [Test]
    public async Task Conflicting_fifo_consumer_identity_bindings_require_an_explicit_send_binding()
    {
        using var client = new SqsReceiveTests.ReceiveClient();
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.AmazonSqs("sqs", sqs =>
            {
                sqs.Options.Configure(options =>
                {
                    options.SqsClient = client;
                    options.Queues.Add(Queue("orders.fifo"));
                    options.Queues.Add(Queue("dead.fifo"));
                });
                sqs.Consume("orders.fifo", consumer =>
                {
                    consumer.Handler<SqsReceiveFlociTests.Handler>();
                    consumer.Options.HeaderNames = new("first_");
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = "dead.fifo"
                    });
                });
                sqs.Consume("orders.fifo", consumer =>
                {
                    consumer.Handler<SqsReceiveFlociTests.Handler>();
                    consumer.Options.HeaderNames = new("second_");
                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = "dead.fifo"
                    });
                });
            });
        });
        await using var provider = services.BuildServiceProvider();

        var thrown = await Assert.That(() => provider.GetRequiredKeyedService<SqsSendTransport>("sqs"))
            .Throws<InvalidOperationException>();
        await Assert.That(thrown!.Message).Contains("consumers must use the same HeaderNames identity binding");
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

    private static IHost Host(IAmazonSQS client, TimeSpan[]? redeliveryDelays = null, Scheduler? scheduler = null,
        string source = "orders", string dead = "dead", string? receivePrefix = null, string? sendPrefix = null)
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

                if (receivePrefix == "ce_")
                {
                    sqs.Send<SqsSendTests.Order>(dead, route =>
                    {
                        route.SendOptions = new()
                        {
                            HeaderNames = new("dead_")
                        };
                        route.OrderingKey(static _ => "group");
                    });
                }

                if (sendPrefix is not null)
                {
                    sqs.Send<SqsReceiveFlociTests.Payload>(source, route =>
                    {
                        route.SendOptions = new()
                        {
                            HeaderNames = new(sendPrefix)
                        };
                        route.OrderingKey(static _ => "group");
                    });
                }

                sqs.Consume(source, consumer =>
                {
                    consumer.Handler<SqsReceiveFlociTests.Handler>();

                    if (receivePrefix is not null)
                    {
                        consumer.Options.HeaderNames = new(receivePrefix);
                    }

                    consumer.Failures(new()
                    {
                        DeadLetterEndpoint = dead,
                        RetryDelays = [],
                        Jitter = 0,
                        RedeliveryDelays = redeliveryDelays ?? []
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
