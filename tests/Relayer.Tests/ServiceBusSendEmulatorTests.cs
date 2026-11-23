using System.Diagnostics.Metrics;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using DotNet.Testcontainers.Builders;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.AzureServiceBus;
using Relayer.Transports;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace Relayer.Tests;

public class ServiceBusSendEmulatorTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Cancellation_during_native_message_configuration_skips_send_metrics(bool batch)
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString());
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        var queue = TestContext.Current!.Isolation.GetIsolatedName("callback-cancellation");
        var options = new ServiceBusTransportOptions { Client = client };
        options.Queues.Add(new(queue));
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        using var duration = new SendDurationCapture(instrumentation);
        long sent = 0;
        using var metrics = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.sent.messages")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            },
        };
        metrics.SetMeasurementEventCallback<long>((_, count, _, _) => Interlocked.Add(ref sent, count));
        metrics.Start();
        await using var transport = new ServiceBusSendTransport("emulator", client, false, options, [queue], TopologyMode.None,
            tracing: tracing);
        await admin.CreateQueueAsync(new CreateQueueOptions(queue) { DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100 }, token);

        try
        {
            using var stop = CancellationTokenSource.CreateLinkedTokenSource(token);
            var sendOptions = new SendOptions().ServiceBus(_ => stop.Cancel());
            var message = new OutgoingMessage(queue, new byte[] { 1 }, options: sendOptions);
            await Assert.That(async () => await (batch ? transport.SendBatchAsync(new[] { message }, stop.Token)
                : transport.SendAsync(message, stop.Token))).Throws<OperationCanceledException>();

            await using var receiver = client.CreateReceiver(queue);
            await Assert.That(await receiver.PeekMessageAsync(cancellationToken: token)).IsNull();
            await Assert.That(sent).IsEqualTo(0);
            await Assert.That(duration.Values.Count).IsEqualTo(0);
        }
        finally
        {
            await admin.DeleteQueueAsync(queue, CancellationToken.None);
        }
    }

    [Test]
    public async Task Rejected_native_batch_can_isolate_oversized_row_from_valid_neighbour()
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString(), new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0 }
        });
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        var queue = TestContext.Current!.Isolation.GetIsolatedName("batch-isolation");
        var options = new ServiceBusTransportOptions { Client = client, AdministrationClient = admin };
        options.Queues.Add(new(queue)
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
        });
        await using var transport = new ServiceBusSendTransport("isolation", client, false, options,
            [queue], TopologyMode.Provision, admin);

        try
        {
            var report = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            await Assert.That(report.Differences.Count).IsEqualTo(0);
            // The rejected row leads the batch, so the healthy neighbour was never admitted.
            var rejected = new OutgoingMessage(queue, new byte[2 * 1024 * 1024]);
            var healthy = new OutgoingMessage(queue, new byte[] { 42 });
            await Assert.That(async () => await transport.SendBatchAsync(new[] { rejected, healthy }, token))
                .Throws<ServiceBusException>();
            await Assert.That(async () => await transport.SendAsync(rejected, token)).Throws<ServiceBusException>();
            await transport.SendAsync(healthy, token);
            await using var receiver = client.CreateReceiver(queue);
            var delivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token);
            await Assert.That(delivered).IsNotNull();
            await Assert.That(delivered!.Body.ToArray()).IsEquivalentTo(new byte[] { 42 });
            await receiver.CompleteMessageAsync(delivered, token);
        }
        finally
        {
            await admin.DeleteQueueAsync(queue, token);
        }
    }

    [Test]
    public async Task Route_callback_sets_native_properties_on_dispatched_rows()
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString());
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        var queue = TestContext.Current!.Isolation.GetIsolatedName("route-native").ToLowerInvariant();
        var destination = queue.ToUpperInvariant();
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.AddMessageTypesFromThisAssembly().ServiceBus("routed", bus =>
        {
            bus.Options.Configure(options =>
            {
                options.Client = client;
                options.Queues.Add(new(queue)
                {
                    DefaultMessageTimeToLive = TimeSpan.FromHours(1),
                    MaxSizeInMegabytes = 100,
                    DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
                });
            });
            bus.Send<ServiceBusSendTests.First>(destination, route => route.ServiceBus(static (_, native) =>
            {
                native.TimeToLive = TimeSpan.FromMinutes(5);
                native.ReplyTo = "replies";
                native.To = "billing";
            }));
        }));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetServices<ITransport>().Single();
        await admin.CreateQueueAsync(new CreateQueueOptions(queue) { DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100 }, token);

        try
        {
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
            // The headers-only shape that outbox dispatch sends for a stored row.
            await transport.SendAsync(new(destination, new byte[] { 7 },
                new KeyValuePair<string, WireString>[] { new("cloudEvents:type", new("servicebus.first;v=1")) }), token);
            await using var receiver = client.CreateReceiver(queue);
            var delivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                ?? throw new InvalidOperationException("Routed send was not delivered.");
            await Assert.That(delivered.TimeToLive).IsEqualTo(TimeSpan.FromMinutes(5));
            await Assert.That(delivered.ReplyTo).IsEqualTo("replies");
            await Assert.That(delivered.To).IsEqualTo("billing");
            await Assert.That(delivered.Subject).IsEqualTo("servicebus.first;v=1");
            await receiver.CompleteMessageAsync(delivered, token);
        }
        finally
        {
            await admin.DeleteQueueAsync(queue, token);
        }
    }

    [Test]
    public async Task Trusted_topic_orders_sends_to_external_session_subscriptions_and_provision_reports_plain_ones()
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString());
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        var topic = TestContext.Current!.Isolation.GetIsolatedName("trusted-topic");
        var provisionOptions = new ServiceBusTransportOptions { Client = client, AdministrationClient = admin };
        provisionOptions.Topics.Add(new(topic)
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10),
            SupportOrdering = true
        });
        provisionOptions.TrustedSessionEntities.Add(topic);
        await using var provision = new ServiceBusSendTransport("provision", client, false, provisionOptions,
            [topic], TopologyMode.Provision, admin);

        try
        {
            await Assert.That((await provision.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
            // Another application owns the subscriptions; this producer declares only the topic.
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(topic, "owned")
            {
                RequiresSession = true,
                DefaultMessageTimeToLive = TimeSpan.FromHours(1),
                AutoDeleteOnIdle = TimeSpan.FromHours(1)
            }, token);

            foreach (var mode in new[] { TopologyMode.Validate, TopologyMode.None })
            {
                var options = new ServiceBusTransportOptions { Client = client };
                options.Topics.Add(new(topic));
                options.TrustedSessionEntities.Add(topic);
                await using var trusted = new ServiceBusSendTransport("trusted", client, false, options, [topic], mode);
                await Assert.That((await trusted.ApplyTopologyAsync(mode, token)).Differences.Count).IsEqualTo(0);
                await Assert.That(trusted.GetCapabilities(topic).OrderedByKey).IsTrue();
                var key = "trusted-" + mode;
                await trusted.SendAsync(new(topic, new byte[] { 8 }, orderingKey: new WireString(key)), token);
                await using var session = await client.AcceptSessionAsync(topic, "owned", key, cancellationToken: token);
                var received = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                    ?? throw new InvalidOperationException("Trusted topic send was not delivered.");
                await Assert.That(received.SessionId).IsEqualTo(key);
                await session.CompleteMessageAsync(received, token);
            }

            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(topic, "plain")
            {
                DefaultMessageTimeToLive = TimeSpan.FromHours(1),
                AutoDeleteOnIdle = TimeSpan.FromHours(1)
            }, token);
            var report = await provision.ApplyTopologyAsync(TopologyMode.Provision, token);
            await Assert.That(report.Differences.Single())
                .Contains($"'{topic}/Subscriptions/plain': RequiresSession expected 'True', actual 'False'");
        }
        finally
        {
            await admin.DeleteTopicAsync(topic, CancellationToken.None);
        }
    }

    [Test]
    public async Task Broker_duplicate_detection_drops_a_resend_with_the_same_envelope_id()
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString());
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        var queue = TestContext.Current!.Isolation.GetIsolatedName("duplicate-detection");
        var options = new ServiceBusTransportOptions { Client = client, AdministrationClient = admin };
        options.Queues.Add(new(queue)
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromSeconds(20)
        });
        await using var transport = new ServiceBusSendTransport("duplicates", client, false, options,
            [queue], TopologyMode.Provision, admin);

        try
        {
            var report = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            await Assert.That(report.Differences.Count).IsEqualTo(0);
            var id = Guid.NewGuid().ToString("D");
            KeyValuePair<string, WireString>[] headers = [new("cloudEvents:id", new(id))];
            // An outbox redispatch after a crash resends the stored row with its original envelope id.
            await transport.SendAsync(new(queue, new byte[] { 1 }, headers), token);
            await transport.SendAsync(new(queue, new byte[] { 2 }, headers), token);
            KeyValuePair<string, WireString>[] other = [new("cloudEvents:id", new(Guid.NewGuid().ToString("D")))];
            await transport.SendAsync(new(queue, new byte[] { 3 }, other), token);
            await using var receiver = client.CreateReceiver(queue);
            var received = new List<ServiceBusReceivedMessage>();

            while (received.Count < 3 && await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5), token) is { } message)
            {
                received.Add(message);
                await receiver.CompleteMessageAsync(message, token);
            }

            await Assert.That(received.Select(message => message.Body.ToArray()[0])).IsEquivalentTo(new byte[] { 1, 3 });
            await Assert.That(received[0].MessageId).IsEqualTo(id);
        }
        finally
        {
            await admin.DeleteQueueAsync(queue, token);
        }
    }

    [Test]
    public async Task Native_sends_batches_sessions_scheduling_and_topology_round_trip()
    {
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        long sent = 0;
        using var metrics = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.sent.messages")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            },
        };
        metrics.SetMeasurementEventCallback<long>((_, count, _, _) => Interlocked.Add(ref sent, count));
        metrics.Start();
        var stack = await SharedBrokers.ServiceBus.Value;
        var emulator = stack.Emulator;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = timeout.Token;
        using var capture = new SendDurationCapture(instrumentation);
        await using var client = new ServiceBusClient(emulator.GetConnectionString());
        var admin = new ServiceBusAdministrationClient(emulator.GetHttpConnectionString());
        var options = new ServiceBusTransportOptions { Client = client, AdministrationClient = admin };
        // Match the emulator's documented TTL/size quotas and its duplicate-window default.
        options.Queues.Add(new("send-orders")
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
        });
        options.Queues.Add(new("send-sessions")
        {
            RequiresSession = true,
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
        });
        options.Topics.Add(new("send-events")
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10),
            SupportOrdering = true
        });
        options.Subscriptions.Add(new("send-events", "audit")
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            AutoDeleteOnIdle = TimeSpan.FromHours(1)
        });

        await using (var transport = new ServiceBusSendTransport("emulator", client, false, options,
            ["send-orders", "send-sessions", "send-events"], TopologyMode.Provision, admin, tracing))
        {
            var provisioned = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);

            foreach (var difference in provisioned.Differences)
            {
                Console.WriteLine(difference);
            }

            await Assert.That(string.Join(Environment.NewLine, provisioned.Differences)).IsEqualTo("");
            var repeated = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            await Assert.That(repeated.Differences.Count).IsEqualTo(0);
            // Validate checks live links without sending a message. Session declarations explicitly remain unresolved.
            var validation = await transport.ApplyTopologyAsync(TopologyMode.Validate, token);
            await Assert.That(validation.Differences.Count).IsEqualTo(1);
            await Assert.That(validation.Differences[0]).Contains("send-sessions");

            KeyValuePair<string, WireString>[] headers = [new("cloudEvents:id", new("original-id")),
                new("cloudEvents:type", new("orders;v=1")), new("traceparent", new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))];
            KeyValuePair<string, WireString>[] contentHeaders = [.. headers, new("content-type", new("application/x-protobuf"))];
            var native = new SendOptions().ServiceBus(message =>
            {
                message.Subject = "native";
                message.ContentType = "application/json";
            });
            await transport.SendAsync(new("send-orders", new byte[] { 42 }, contentHeaders, options: native), token);
            await Assert.That(sent).IsEqualTo(1);
            await using var orders = client.CreateReceiver("send-orders");
            var order = await orders.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                ?? throw new InvalidOperationException("Queue send was not delivered.");
            await Assert.That(order.Body.ToMemory().Span[0]).IsEqualTo((byte)42);
            await Assert.That(order.Subject).IsEqualTo("native");
            await Assert.That(order.ContentType).IsEqualTo("application/x-protobuf");
            await Assert.That(order.ApplicationProperties.ContainsKey("content-type")).IsFalse();

            foreach (var header in headers)
            {
                await Assert.That(order.ApplicationProperties[header.Key]).IsEqualTo(header.Value.ToString());
            }

            await orders.CompleteMessageAsync(order, token);

            await transport.SendBatchAsync(new[] {
                new OutgoingMessage("send-sessions", new byte[] { 1 }, orderingKey: new WireString("customer")),
                new OutgoingMessage("send-sessions", new byte[] { 2 }, orderingKey: new WireString("customer")),
                new OutgoingMessage("send-events", new byte[] { 3 }, contentHeaders) }, token);
            await Assert.That(sent).IsEqualTo(4);
            await using var session = await client.AcceptSessionAsync("send-sessions", "customer", cancellationToken: token);

            for (byte expected = 1; expected <= 2; expected++)
            {
                var message = await session.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                    ?? throw new InvalidOperationException("Session batch was not delivered.");
                await Assert.That(message.SessionId).IsEqualTo("customer");
                await Assert.That(message.Body.ToMemory().Span[0]).IsEqualTo(expected);
                await session.CompleteMessageAsync(message, token);
            }

            await using var audit = client.CreateReceiver("send-events", "audit");
            var published = await audit.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                ?? throw new InvalidOperationException("Topic send was not delivered to the subscription.");
            await Assert.That(published.Body.ToMemory().Span[0]).IsEqualTo((byte)3);
            await Assert.That(published.Subject).IsEqualTo("orders;v=1");
            await Assert.That(published.ContentType).IsEqualTo("application/x-protobuf");
            await Assert.That(published.ApplicationProperties.ContainsKey("content-type")).IsFalse();
            await audit.CompleteMessageAsync(published, token);

            var scheduledAt = DateTimeOffset.UtcNow.AddSeconds(2);
            await transport.SendAsync(new("send-orders", new byte[] { 99 }, deliverAt: scheduledAt), token);
            await Assert.That(sent).IsEqualTo(5);
            var scheduled = await orders.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                ?? throw new InvalidOperationException("Scheduled send was not delivered.");
            await Assert.That(scheduled.EnqueuedTime >= DateTimeOffset.FromUnixTimeMilliseconds(scheduledAt.ToUnixTimeMilliseconds())).IsTrue();
            await Assert.That(scheduled.Body.ToMemory().Span[0]).IsEqualTo((byte)99);
            await orders.CompleteMessageAsync(scheduled, token);

            await Assert.That(capture.Values.Count).IsEqualTo(4);
            await Assert.That(capture.Values.All(value => value.Seconds > 0 &&
                Equals(value.Tags["messaging.system"], "servicebus") &&
                Equals(value.Tags["server.address"], client.FullyQualifiedNamespace) && !value.Tags.ContainsKey("error.type"))).IsTrue();

            // Externally managed session queues work with least-privilege topology modes.
            foreach (var mode in new[] { TopologyMode.Validate, TopologyMode.None })
            {
                var trustedOptions = new ServiceBusTransportOptions { Client = client };
                trustedOptions.Queues.Add(new("send-sessions")
                {
                    RequiresSession = true
                });
                trustedOptions.TrustedSessionEntities.Add("SEND-SESSIONS");
                ServiceBusSendTransport.ValidateDeclarations("trusted", trustedOptions, ["send-sessions"]);
                await using var trusted = new ServiceBusSendTransport("trusted", client, false, trustedOptions,
                    ["send-sessions"], mode);
                await Assert.That((await trusted.ApplyTopologyAsync(mode, token)).Differences.Count).IsEqualTo(0);
                await Assert.That(trusted.GetCapabilities("send-sessions").OrderedByKey).IsTrue();
                var key = "trusted-" + mode;
                await trusted.SendAsync(new("send-sessions", new byte[] { 8 }, orderingKey: new WireString(key)), token);
                await using var trustedSession = await client.AcceptSessionAsync("send-sessions", key, cancellationToken: token);
                var received = await trustedSession.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                    ?? throw new InvalidOperationException("Trusted session send was not delivered.");
                await Assert.That(received.SessionId).IsEqualTo(key);
                await trustedSession.CompleteMessageAsync(received, token);

                var untrustedOptions = new ServiceBusTransportOptions { Client = client };
                untrustedOptions.Queues.Add(new("send-sessions")
                {
                    RequiresSession = true
                });
                await using var untrusted = new ServiceBusSendTransport("untrusted", client, false, untrustedOptions,
                    ["send-sessions"], mode);
                await Assert.That(async () => await untrusted.SendAsync(new("send-sessions", default,
                    orderingKey: new WireString(key)), token)).Throws<InvalidOperationException>()
                    .WithMessageContaining("TrustedSessionEntities");
            }

            // Existing properties are reported, never updated or replaced.
            options.Queues[0].MaxDeliveryCount = 11;
            var drift = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            await Assert.That(drift.Differences.Single()).Contains("MaxDeliveryCount expected '11', actual '10'");
            await Assert.That((await admin.GetQueueAsync("send-orders", token)).Value.MaxDeliveryCount).IsEqualTo(10);
        }
        // Disposing the adapter closes its senders, while the caller-owned native client remains usable.
        await using var senderAfterDisposal = client.CreateSender("send-orders");
        await senderAfterDisposal.SendMessageAsync(new(new byte[] { 7 }), token);
    }
}
