using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Transports;
using Testcontainers.ServiceBus;

namespace Relayer.Tests;

public partial class ServiceBusBindingTests
{
    [Test]
    public async Task Native_settlement_round_trips_against_service_bus_emulator()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = timeout.Token;
        await using var emulator = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:1.1.2")
            .WithAcceptLicenseAgreement(true)
            .WithConfig(Path.Combine(AppContext.BaseDirectory, "servicebus-config.json"))
            .Build();
        await emulator.StartAsync(token);
        await using var client = new ServiceBusClient(emulator.GetConnectionString());
        await using var sender = client.CreateSender("settlement");
        await using var receiver = client.CreateReceiver("settlement");
        await using var deadLetters = client.CreateReceiver("settlement", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var pipeline = Pipeline(provider);
        var properties = new Dictionary<string, object> { ["settlement-test"] = "preserved" };

        foreach (var operation in new[] { "complete", "abandon", "defer", "dead_letter" })
        {
            var outgoing = new ServiceBusMessage(new BinaryData(new byte[] { 42 })) { MessageId = operation };
            outgoing.ApplicationProperties["cloudEvents:id"] = operation;
            outgoing.ApplicationProperties["cloudEvents:type"] = "servicebus.payload;v=1";
            outgoing.ApplicationProperties["traceparent"] = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
            await sender.SendMessageAsync(outgoing, token);
            var incoming = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                ?? throw new InvalidOperationException($"No native delivery for {operation}.");
            control.Handle = async context =>
            {
                await Assert.That(context.MessageId).IsEqualTo(operation);
                await Assert.That(context.Attempt).IsEqualTo(1);
                await Assert.That(ServiceBusBinding.GetBody(context.ServiceBus().Callback).First.Span[0]).IsEqualTo((byte)42);
                await Settle(context.ServiceBus(), operation, properties);
            };
            var args = new ProcessMessageEventArgs(incoming, receiver, "emulator", token);
            await Assert.That((await pipeline.ProcessAsync(args, token)).Kind).IsEqualTo(DispositionKind.SettledByHandler);

            ServiceBusReceivedMessage? result = null;
            if (operation == "abandon")
            {
                result = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                    ?? throw new InvalidOperationException("Abandoned message was not redelivered.");
                await Assert.That(result.DeliveryCount).IsEqualTo(2);
            }
            else if (operation == "defer")
                result = await receiver.ReceiveDeferredMessageAsync(incoming.SequenceNumber, token);
            else if (operation == "dead_letter")
            {
                result = await deadLetters.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
                    ?? throw new InvalidOperationException("Native DLQ is empty.");
                await Assert.That(result.DeadLetterReason).IsEqualTo("poison");
                await Assert.That(result.DeadLetterErrorDescription).IsEqualTo("details");
            }
            if (result is not null)
            {
                await Assert.That(result.MessageId).IsEqualTo(operation);
                await Assert.That(result.ApplicationProperties["settlement-test"]).IsEqualTo("preserved");
                await Assert.That(result.ApplicationProperties["traceparent"]).IsEqualTo(outgoing.ApplicationProperties["traceparent"]);
                await (operation == "dead_letter" ? deadLetters : receiver).CompleteMessageAsync(result, token);
            }
        }
        await Assert.That(await receiver.PeekMessageAsync(cancellationToken: token)).IsNull();
        await Assert.That(await deadLetters.PeekMessageAsync(cancellationToken: token)).IsNull();
    }
}
