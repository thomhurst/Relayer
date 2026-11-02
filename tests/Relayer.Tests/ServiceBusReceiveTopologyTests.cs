using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusReceiveTopologyTests
{
    [Test]
    public async Task Receive_only_validation_uses_Listen_peeks_without_opening_Send_links()
    {
        var client = new ListenClient();
        var options = new AzureServiceBusTransportOptions { Client = client };
        options.Queues.Add(new("orders"));
        options.Topics.Add(new("events"));
        options.Subscriptions.Add(new("events", "audit"));
        var queue = new ConsumerDefinition<ServiceBusConsumerOptions>("orders", new(), [], 1, 1, TimeSpan.FromMilliseconds(100));
        var subscription = new ConsumerDefinition<ServiceBusConsumerOptions>("events", new() { Subscription = "audit" }, [], 1, 1, TimeSpan.FromMilliseconds(100));
        await using var transport = new ServiceBusSendTransport("listen", client, false, options, [], TopologyMode.Validate,
            consumers: [queue, subscription]);
        var report = await transport.ApplyTopologyAsync(TopologyMode.Validate, default);
        await Assert.That(report.Differences.Count).IsEqualTo(0);
        await Assert.That(client.Receiver.Peeks).IsEqualTo(2);
        await Assert.That(transport.GetCapabilities("orders").DeadLetter).IsTrue();
        await Assert.That(transport.GetCapabilities("events").MaxRedeliveryDelay).IsEqualTo(TimeSpan.Zero);
    }

    [Test]
    public async Task Delayed_redelivery_fails_at_composition_without_starting_a_processor()
    {
        var client = new ServiceBusReceiveTests.NativeClient();
        await using var provider = ServiceBusReceiveTests.Provider(client, consumer => consumer.Options.Failure = new()
        { RedeliveryDelays = new[] { TimeSpan.FromSeconds(1) } });
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
        internal ServiceBusSendTests.NativeReceiver Receiver { get; } = new();
        public override ServiceBusSender CreateSender(string queueOrTopicName) =>
            throw new InvalidOperationException("A receive-only identity has no Send permission.");
        public override ServiceBusReceiver CreateReceiver(string queueName) => Receiver;
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions? options = null) => Receiver;
        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions? options = null) => Receiver;
    }
}
