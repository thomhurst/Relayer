using Relayer.Transports;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using DotNet.Testcontainers.Builders;
using Relayer.AzureServiceBus;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace Relayer.Tests;

public class ServiceBusSendEmulatorTests
{
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
        var queue = "batch-isolation-" + Guid.NewGuid().ToString("N");
        var options = new AzureServiceBusTransportOptions { Client = client, AdministrationClient = admin };
        options.Queues.Add(new(queue) { DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10) });
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
        finally { await admin.DeleteQueueAsync(queue, token); }
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
                    listener.EnableMeasurementEvents(instrument);
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
        options.Queues.Add(new("send-orders") { DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100, DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10) });
        options.Queues.Add(new("send-sessions") { RequiresSession = true, DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100, DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10) });
        options.Topics.Add(new("send-events") { DefaultMessageTimeToLive = TimeSpan.FromHours(1), MaxSizeInMegabytes = 100, DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10), SupportOrdering = true });
        options.Subscriptions.Add(new("send-events", "audit") { DefaultMessageTimeToLive = TimeSpan.FromHours(1), AutoDeleteOnIdle = TimeSpan.FromHours(1) });
        await using (var transport = new ServiceBusSendTransport("emulator", client, false, options,
            ["send-orders", "send-sessions", "send-events"], TopologyMode.Provision, admin, tracing))
        {
            var provisioned = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
            foreach (var difference in provisioned.Differences) Console.WriteLine(difference);
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
            var native = new SendOptions().ServiceBus(message => { message.Subject = "native"; message.ContentType = "application/json"; });
            await transport.SendAsync(new("send-orders", new byte[] { 42 }, contentHeaders, options: native), token);
            await Assert.That(sent).IsEqualTo(1);
            await using var orders = client.CreateReceiver("send-orders");
            var order = await orders.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                ?? throw new InvalidOperationException("Queue send was not delivered.");
            await Assert.That(order.Body.ToMemory().Span[0]).IsEqualTo((byte)42);
            await Assert.That(order.Subject).IsEqualTo("native");
            await Assert.That(order.ContentType).IsEqualTo("application/x-protobuf");
            await Assert.That(order.ApplicationProperties.ContainsKey("content-type")).IsFalse();
            foreach (var header in headers) await Assert.That(order.ApplicationProperties[header.Key]).IsEqualTo(header.Value.ToString());
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
                trustedOptions.Queues.Add(new("send-sessions") { RequiresSession = true });
                trustedOptions.TrustedSessionEntities.Add("send-sessions");
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
                untrustedOptions.Queues.Add(new("send-sessions") { RequiresSession = true });
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
