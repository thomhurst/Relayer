using System.Text.Json.Serialization;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Transports;

namespace Relayer.Tests;

// #853: with TopologyMode.None and no declarations, Relayer sends to and consumes externally provisioned entities.
public sealed class ServiceBusImpliedTopologyEmulatorTests
{
    [Test]
    public async Task None_without_declarations_sends_and_consumes_provisioned_queue_subscription_and_sessions()
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = timeout.Token;
        var isolation = TestContext.Current!.Isolation;
        var queue = isolation.GetIsolatedName("implied-orders");
        var sessions = isolation.GetIsolatedName("implied-sessions");
        var topic = isolation.GetIsolatedName("implied-events");
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        await stack.CreateQueueAsync(queue, false, token);
        await stack.CreateQueueAsync(sessions, true, token);
        await admin.CreateTopicAsync(new CreateTopicOptions(topic)
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
        }, token);
        await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(topic, "audit")
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            LockDuration = TimeSpan.FromMinutes(1),
            MaxDeliveryCount = 10
        }, token);

        try
        {
            await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString());
            var received = Channel.CreateUnbounded<string>();
            var services = new ServiceCollection();
            services.AddSingleton(received);
            services.AddRelayer(builder =>
            {
                builder.Infrastructure().MessageTypes.Add<ImpliedPayload>("servicebus.implied");
                builder.UseJsonSerializer(ServiceBusImpliedTopologyEmulatorJson.Default);
                builder.ServiceBus("bus", bus =>
                {
                    bus.TopologyMode = TopologyMode.None;
                    bus.Options.Configure(options => options.Client = client);
                    bus.Send<ImpliedPayload>(queue);
                    bus.Consume(queue, consumer => consumer.Handler<ImpliedHandler>());
                    bus.Consume(topic, consumer =>
                    {
                        consumer.Options.Subscription = "audit";
                        consumer.Handler<ImpliedHandler>();
                    });
                    bus.Consume(sessions, consumer =>
                    {
                        consumer.Options.SessionProcessorOptions = new()
                        {
                            AutoCompleteMessages = false,
                            MaxConcurrentSessions = 1
                        };
                        consumer.Handler<ImpliedHandler>();
                    });
                });
            });
            await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
            await ServiceBusReceiveTests.Start(provider, token);

            using (var scope = provider.CreateScope())
            {
                await scope.ServiceProvider.GetRequiredService<ISender>().SendWithoutOutboxAsync(new ImpliedPayload(1), token);
            }

            await using (var sender = client.CreateSender(topic))
            {
                await sender.SendMessageAsync(Message(2), token);
            }

            await using (var sender = client.CreateSender(sessions))
            {
                await sender.SendMessageAsync(Message(3, "customer"), token);
            }

            var delivered = new List<string>();

            for (var i = 0; i < 3; i++)
            {
                delivered.Add(await received.Reader.ReadAsync(token));
            }

            await Assert.That(delivered.Order().ToArray()).IsEquivalentTo(new[] { "1/", "2/", "3/customer" });
            await ServiceBusReceiveTests.Stop(provider, token);
        }
        finally
        {
            await admin.DeleteQueueAsync(queue, CancellationToken.None);
            await admin.DeleteQueueAsync(sessions, CancellationToken.None);
            await admin.DeleteTopicAsync(topic, CancellationToken.None);
        }
    }

    [Test]
    [Arguments(TopologyMode.Validate)]
    [Arguments(TopologyMode.None)]
    public async Task Mixed_case_subscription_consumer_receives_from_the_declared_topic(TopologyMode mode)
    {
        var stack = await SharedBrokers.ServiceBus.Value;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        var topicName = TestContext.Current!.Isolation.GetIsolatedName("case-events").ToLowerInvariant();
        var topic = new CreateTopicOptions(topicName)
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10)
        };
        var subscription = new CreateSubscriptionOptions(topicName, "billing")
        {
            DefaultMessageTimeToLive = TimeSpan.FromHours(1)
        };
        var admin = new ServiceBusAdministrationClient(stack.Emulator.GetHttpConnectionString());
        await admin.CreateTopicAsync(topic, token);

        try
        {
            await admin.CreateSubscriptionAsync(subscription, token);
            await using var client = new ServiceBusClient(stack.Emulator.GetConnectionString());
            var received = Channel.CreateUnbounded<string>();
            var services = new ServiceCollection();
            services.AddSingleton(received);
            services.AddRelayer(builder =>
            {
                builder.Infrastructure().MessageTypes.Add<ImpliedPayload>("servicebus.implied");
                builder.UseJsonSerializer(ServiceBusImpliedTopologyEmulatorJson.Default);
                builder.ServiceBus("bus", bus =>
                {
                    bus.TopologyMode = mode;
                    bus.Options.Configure(options =>
                    {
                        options.Client = client;
                        options.Topics.Add(topic);
                        options.Subscriptions.Add(subscription);
                    });
                    bus.Consume(topicName.ToUpperInvariant(), consumer =>
                    {
                        consumer.Options.Subscription = "BILLING";
                        consumer.Handler<ImpliedHandler>();
                    });
                });
            });
            await using var provider = services.BuildServiceProvider();
            await ServiceBusReceiveTests.Start(provider, token);

            try
            {
                await using var sender = client.CreateSender(topicName);
                await sender.SendMessageAsync(Message(42), token);
                await Assert.That(await received.Reader.ReadAsync(token)).IsEqualTo("42/");
            }
            finally
            {
                await ServiceBusReceiveTests.Stop(provider, token);
            }
        }
        finally
        {
            await admin.DeleteTopicAsync(topicName, CancellationToken.None);
        }
    }

    private static ServiceBusMessage Message(int value, string? session = null)
    {
        var message = new ServiceBusMessage(new BinaryData($"{{\"Value\":{value}}}")) { SessionId = session, ContentType = "application/json" };
        message.ApplicationProperties["cloudEvents:id"] = Guid.NewGuid().ToString("N");
        message.ApplicationProperties["cloudEvents:type"] = "servicebus.implied;v=1";
        return message;
    }

    [Message("servicebus.implied")]
    public sealed record ImpliedPayload(int Value);

    public sealed class ImpliedHandler(Channel<string> received) : IHandler<ImpliedPayload>
    {
        public ValueTask HandleAsync(ImpliedPayload message, MessageContext context, CancellationToken cancellationToken)
        {
            received.Writer.TryWrite(message.Value + "/" + context.ServiceBus().Message.SessionId);
            return ValueTask.CompletedTask;
        }
    }
}

[JsonSerializable(typeof(ServiceBusImpliedTopologyEmulatorTests.ImpliedPayload))]
internal partial class ServiceBusImpliedTopologyEmulatorJson : JsonSerializerContext;
