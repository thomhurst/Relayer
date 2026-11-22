using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class ServiceBusReceiveTopologyTests
{
    [Test]
    public async Task Receive_only_validation_uses_Listen_peeks_without_opening_Send_links()
    {
        var client = new ListenClient();
        var options = new ServiceBusTransportOptions { Client = client };
        options.Queues.Add(new("orders"));
        options.Topics.Add(new("events"));
        options.Subscriptions.Add(new("events", "audit"));
        var queue = new ConsumerDefinition<ServiceBusConsumerOptions>("orders", new(), [], 1, 1, TimeSpan.FromMilliseconds(100));
        var subscription = new ConsumerDefinition<ServiceBusConsumerOptions>("events", new()
        {
            Subscription = "audit"
        }, [], 1, 1, TimeSpan.FromMilliseconds(100));
        await using var transport = new ServiceBusSendTransport("listen", client, false, options, [], TopologyMode.Validate,
            consumers: [queue, subscription]);
        var report = await transport.ApplyTopologyAsync(TopologyMode.Validate, default);
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        await Assert.That(client.Receiver.Peeks).IsEqualTo(2);
        await Assert.That(transport.GetCapabilities("orders").DeadLetter).IsTrue();
        await Assert.That(transport.GetCapabilities("events").MaxRedeliveryDelay).IsEqualTo(TimeSpan.Zero);
    }

    [Test]
    public async Task Validate_rejects_missing_Listen_access_on_a_queue_used_for_sending()
    {
        var client = new ServiceBusSendTests.NativeClient();
        client.Receiver.PeekFailure = new UnauthorizedAccessException("Listen claim is required.");
        var options = new ServiceBusTransportOptions { Client = client };
        options.Queues.Add(new("orders"));
        var consumer = new ConsumerDefinition<ServiceBusConsumerOptions>("orders", new(), [], 1, 1, TimeSpan.FromMilliseconds(100));
        await using var transport = new ServiceBusSendTransport("bus", client, false, options, ["orders"], TopologyMode.Validate,
            consumers: [consumer]);

        await Assert.That(async () => await transport.ApplyTopologyAsync(TopologyMode.Validate, default))
            .Throws<UnauthorizedAccessException>().WithMessageContaining("Listen claim is required.");

        await Assert.That(client.Senders.Sum(sender => sender.BatchCreations)).IsEqualTo(1);
        await Assert.That(client.Receiver.Peeks).IsEqualTo(1);
    }

    [Test]
    public async Task Validate_probes_a_queue_used_for_sending_and_receiving_without_consuming_messages()
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var queue = TestContext.Current!.Isolation.GetIsolatedName("validate-dual-role");
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        await stack.CreateQueueAsync(queue, false, token);

        try
        {
            await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString());
            var options = new ServiceBusTransportOptions { Client = client };
            options.Queues.Add(new(queue));
            var consumer = new ConsumerDefinition<ServiceBusConsumerOptions>(queue, new(), [], 1, 1, TimeSpan.FromMilliseconds(100));
            await using var transport = new ServiceBusSendTransport("bus", client, false, options, [queue], TopologyMode.Validate,
                consumers: [consumer]);
            await using var sender = client.CreateSender(queue);
            await sender.SendMessageAsync(new ServiceBusMessage("orders"), token);

            var report = await transport.ApplyTopologyAsync(TopologyMode.Validate, token);

            await Assert.That(report.Differences.Count).IsEqualTo(0);
            await using var receiver = client.CreateReceiver(queue);
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), token)
                ?? throw new InvalidOperationException("Validation consumed the queued message.");
            await Assert.That(message.Body.ToString()).IsEqualTo("orders");
            await Assert.That(message.DeliveryCount).IsEqualTo(1);
            await receiver.CompleteMessageAsync(message, token);
        }
        finally
        {
            await admin.DeleteQueueAsync(queue, CancellationToken.None);
        }
    }

    [Test]
    public async Task Trusted_session_subscription_still_validates_a_declared_consumers_Listen_link()
    {
        var client = new ListenClient();
        var options = new ServiceBusTransportOptions { Client = client };
        options.Topics.Add(new("events"));
        options.Subscriptions.Add(new("events", "audit")
        {
            RequiresSession = true
        });
        options.TrustedSessionEntities.Add("events/Subscriptions/audit");
        var subscription = new ConsumerDefinition<ServiceBusConsumerOptions>("events", new()
        {
            Subscription = "audit"
        },
            [], 1, 1, TimeSpan.FromMilliseconds(100));
        await using var transport = new ServiceBusSendTransport("listen", client, false, options, [], TopologyMode.Validate,
            consumers: [subscription]);
        var report = await transport.ApplyTopologyAsync(TopologyMode.Validate, default);
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        await Assert.That(client.Receiver.Peeks).IsEqualTo(1);
    }

    [Test]
    public async Task Delayed_redelivery_fails_at_composition_without_starting_a_processor()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, consumer => consumer.Failures(new()
        {
            RedeliveryDelays = new[] { TimeSpan.FromSeconds(1) }
        }));
        await Assert.That(async () => await ServiceBusReceiveTests.Start(provider)).Throws<InvalidOperationException>();
        await Assert.That(client.Processors.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Native_concurrency_is_not_silently_overwritten()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        var options = new ServiceBusProcessorOptions { AutoCompleteMessages = false, MaxConcurrentCalls = 2 };
        await using var provider = ServiceBusReceiveTests.Provider(client, consumer => consumer.Options.ProcessorOptions = options);
        await Assert.That(async () => await ServiceBusReceiveTests.Start(provider)).Throws<InvalidOperationException>();
        await Assert.That(options.MaxConcurrentCalls).IsEqualTo(2);
        await Assert.That(client.Processors.Count).IsEqualTo(0);
    }

    private sealed class ListenClient : ServiceBusClient
    {
        internal ServiceBusSendTests.NativeReceiver Receiver
        {
            get;
        } = new();

        public override ServiceBusSender CreateSender(string queueOrTopicName)
        {
            throw new InvalidOperationException("A receive-only identity has no Send permission.");
        }

        public override ServiceBusReceiver CreateReceiver(string queueName)
        {
            return Receiver;
        }

        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions? options = null)
        {
            return Receiver;
        }

        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions? options = null)
        {
            return Receiver;
        }
    }
}
