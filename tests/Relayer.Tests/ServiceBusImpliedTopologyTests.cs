using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.AzureServiceBus;
using Relayer.Transports;
using static Relayer.Tests.ServiceBusReceiveTests;
using SendClient = Relayer.Tests.ServiceBusSendTests.NativeClient;

namespace Relayer.Tests;

// #853: with TopologyMode.None, routes and consumers imply the entities they name.
public sealed class ServiceBusImpliedTopologyTests
{
    [Test]
    [Arguments(TopologyMode.None)]
    [Arguments(TopologyMode.Validate)]
    public async Task Queue_consumer_reuses_explicit_declaration_with_different_casing(TopologyMode mode)
    {
        var client = new NativeClient();
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Payload>("servicebus.receive-test");
            builder.UseJsonSerializer(ServiceBusReceiveTestJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = mode;
                bus.Options.Configure(options =>
                {
                    options.Client = client;
                    options.Queues.Add(new("orders"));
                });
                bus.Consume("ORDERS", consumer => consumer.Handler<Handler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        await Start(provider);
        await Assert.That(client.Processors.Single().Source).IsEqualTo("ORDERS");

        using var output = new MemoryStream();
        provider.GetRequiredService<ServiceBusTopologyExporter>().Export("bus", output, ServiceBusTopologyFormat.Bicep);
        await Assert.That(System.Text.Encoding.UTF8.GetString(output.ToArray())).Contains("name: 'orders'");
        await Stop(provider);
    }

    [Test]
    [Arguments("events", "billing")]
    [Arguments("EVENTS", "billing")]
    [Arguments("events", "BILLING")]
    [Arguments("EVENTS", "BILLING")]
    public async Task None_reuses_the_explicit_subscription_and_checks_its_delivery_limit(string topic, string subscription)
    {
        var client = new NativeClient();
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Payload>("servicebus.receive-test");
            builder.UseJsonSerializer(ServiceBusReceiveTestJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options =>
                {
                    options.Client = client;
                    options.Topics.Add(new("events"));
                    options.Subscriptions.Add(new("events", "billing")
                    {
                        MaxDeliveryCount = 1
                    });
                });
                bus.Consume(topic, consumer =>
                {
                    consumer.Handler<Handler>().Failures(new()
                    {
                        RedeliveryDelays = [TimeSpan.Zero]
                    });
                    consumer.Options.Subscription = subscription;
                });
            });
        });
        await using var provider = services.BuildServiceProvider();

        await Assert.That(async () => await Start(provider)).Throws<InvalidOperationException>()
            .WithMessageContaining("declared MaxDeliveryCount is 1");
        await Assert.That(client.Processors.Count).IsEqualTo(0);
    }

    [Test]
    public async Task None_consumers_imply_their_queue_subscription_and_session_requirement()
    {
        var client = new NativeClient();
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Payload>("servicebus.receive-test");
            builder.UseJsonSerializer(ServiceBusReceiveTestJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options => options.Client = client);
                bus.Consume("orders", consumer => consumer.Handler<Handler>());
                bus.Consume("events", consumer =>
                {
                    consumer.Handler<Handler>();
                    consumer.Options.Subscription = "billing";
                });
                bus.Consume("sessions", consumer =>
                {
                    consumer.Handler<Handler>();
                    consumer.Options.SessionProcessorOptions = new()
                    {
                        AutoCompleteMessages = false,
                        MaxConcurrentSessions = 1
                    };
                });
            });
        });
        await using var provider = services.BuildServiceProvider();
        await Start(provider);
        await Assert.That(client.Processors.Select(processor => processor.Source + "/" + processor.Subscription).Order().ToArray())
            .IsEquivalentTo(new[] { "events/billing", "orders/" });
        await Assert.That(client.SessionProcessors.Count).IsEqualTo(1);
        // The caller's options stay as written, so exporters still see only explicit declarations.
        var options = provider.GetRequiredService<IOptionsMonitor<ServiceBusTransportOptions>>().Get("bus");
        await Assert.That(options.Queues.Count + options.Topics.Count + options.Subscriptions.Count).IsEqualTo(0);
        await Stop(provider);
    }

    [Test]
    public async Task None_send_destinations_are_implied_and_a_trusted_name_is_a_session_topic()
    {
        var client = new SendClient();
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.ServiceBus("bus", bus =>
        {
            bus.TopologyMode = TopologyMode.None;
            bus.Options.Configure(options =>
            {
                options.Client = client;
                options.TrustedSessionEntities.Add("events");
            });
            bus.Send<ServiceBusSendTests.First>("orders");
            bus.Send<ServiceBusSendTests.Second>("events");
        }));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetServices<ITransport>().Single();
        await Assert.That(client.Senders.Select(sender => sender.Entity).ToArray()).IsEquivalentTo(new[] { "orders", "events" });
        await Assert.That(transport.GetCapabilities("orders").OrderedByKey).IsFalse();
        await Assert.That(transport.GetCapabilities("events").OrderedByKey).IsTrue();
    }

    [Test]
    public async Task None_keeps_explicit_declarations_and_rejects_a_session_consumer_on_a_plain_queue()
    {
        var client = new NativeClient();
        await using var provider = Provider(client,
            consumer => consumer.Options.SessionProcessorOptions = new() { AutoCompleteMessages = false });
        await Assert.That(async () => await Start(provider)).Throws<InvalidOperationException>()
            .WithMessageContaining("SessionProcessorOptions must be supplied exactly when");
    }

    [Test]
    public async Task Validate_still_requires_declarations()
    {
        var client = new NativeClient();
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Payload>("servicebus.receive-test");
            builder.UseJsonSerializer(ServiceBusReceiveTestJson.Default);
            builder.ServiceBus("bus", bus =>
            {
                bus.Options.Configure(options => options.Client = client);
                bus.Consume("orders", consumer => consumer.Handler<Handler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        await Assert.That(async () => await Start(provider)).Throws<InvalidOperationException>()
            .WithMessageContaining("declare the native queue or topic/subscription");
    }
}
