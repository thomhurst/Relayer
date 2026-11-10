using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.AzureServiceBus;
using Relayer.HealthChecks;

internal static class ServiceBusReceiversSmoke
{
    internal static async Task RunAsync()
    {
        var client = new NativeClient();
        var services = new ServiceCollection().AddLogging();
        services.AddRelayerServiceBusHealthProviders("aot-servicebus");
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Payload>("aot.servicebus.receive");
            builder.UseJsonSerializer(ServiceBusReceiversJson.Default);
            builder.ServiceBus("aot-servicebus", bus =>
            {
                bus.TopologyMode = TopologyMode.None;
                bus.Options.Configure(options => { options.Client = client; options.Queues.Add(new("orders")); });
                bus.Consume("orders", consumer =>
                {
                    consumer.Options.CircuitBreaker = new();
                    consumer.Handler<Handler>();
                });
            });
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
        foreach (var service in hosted) await service.StartAsync(default);
        var incoming = ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("{\"Value\":42}"), deliveryCount: 1,
            properties: new Dictionary<string, object> { ["cloudEvents:id"] = "smoke", ["cloudEvents:type"] = "aot.servicebus.receive;v=1" });
        await client.Processor.EmitAsync(new(incoming, client.Receiver, "aot", default));
        if (client.Receiver.Completed != 1) throw new InvalidOperationException("Service Bus composed native settlement failed under Native AOT.");
        var probe = provider.GetRequiredKeyedService<ITransportHealthProbe>("aot-servicebus");
        if ((await probe.CheckAsync(default)).Status != HealthStatus.Healthy)
            throw new InvalidOperationException("Service Bus optional native health failed under Native AOT.");
        var consumer = provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        var health = provider.GetRequiredKeyedService<IConsumerHealthProvider>("aot-servicebus");
        if ((await health.GetStateAsync(consumer, default))?.IsIdle != true)
            throw new InvalidOperationException("Service Bus receiver liveness failed under Native AOT.");
        var deadLetters = provider.GetRequiredKeyedService<IDeadLetterManager>("aot-servicebus");
        if (deadLetters.List().Single().Id != "orders") throw new InvalidOperationException("Missing AOT DLQ source.");
        var page = await deadLetters.PeekAsync("orders");
        if (page.Messages.Single().Features.GetRequired<ServiceBusReceivedMessage>().MessageId != "dead")
            throw new InvalidOperationException("Service Bus DLQ snapshot failed under Native AOT.");
        if (await deadLetters.RedriveAsync("orders", message => message.MessageId == "dead") != 1 || client.Sender.Sends != 1)
            throw new InvalidOperationException("Service Bus DLQ redrive failed under Native AOT.");
        foreach (var service in hosted.Reverse()) await service.StopAsync(default);
    }

    [Message("aot.servicebus.receive")]
    public sealed record Payload(int Value);
    public sealed class Handler : IHandler<Payload>
    {
        public async ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            if (message.Value != 42) throw new InvalidOperationException("Unexpected Service Bus Native AOT payload.");
            await context.ServiceBus().CompleteAsync();
        }
    }

    private sealed class NativeClient : ServiceBusClient
    {
        public override string FullyQualifiedNamespace => "aot.servicebus.windows.net";
        internal NativeProcessor Processor { get; } = new();
        internal NativeReceiver Receiver { get; } = new();
        public override ServiceBusProcessor CreateProcessor(string queueName, ServiceBusProcessorOptions options) => Processor;
        public override ServiceBusReceiver CreateReceiver(string queueName) => Receiver;
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => Receiver;
        internal NativeSender Sender { get; } = new();
        public override ServiceBusSender CreateSender(string queueOrTopicName) => Sender;
    }
    private sealed class NativeProcessor : ServiceBusProcessor
    {
        internal Task EmitAsync(ProcessMessageEventArgs args) => OnProcessMessageAsync(args);
        public override Task StartProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task StopProcessingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override Task CloseAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }
    private sealed class NativeSender : ServiceBusSender
    {
        internal int Sends;
        public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
        { Sends++; return Task.CompletedTask; }
        public override ValueTask DisposeAsync() => default;
    }
    private sealed class NativeReceiver : ServiceBusReceiver
    {
        internal int Completed;
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>([ServiceBusModelFactory.ServiceBusReceivedMessage(new BinaryData("{}"), messageId: "dead")]);
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
            => PeekMessagesAsync(maxMessages, cancellationToken: cancellationToken);
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        { Completed++; return Task.CompletedTask; }
        public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default)
            => Task.FromResult<ServiceBusReceivedMessage>(null!);
        public override ValueTask DisposeAsync() => default;
    }
}

[JsonSerializable(typeof(ServiceBusReceiversSmoke.Payload))]
internal partial class ServiceBusReceiversJson : JsonSerializerContext;
