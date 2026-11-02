using System.Diagnostics.Tracing;
using Azure.Core.Diagnostics;
using Azure.Messaging.ServiceBus;
using DotNet.Testcontainers.Builders;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Transports;
using Testcontainers.MsSql;
using Testcontainers.ServiceBus;

namespace Relayer.Tests;

public partial class ServiceBusBindingTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_settlement_round_trips_against_service_bus_emulator(bool nativeOnly)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = timeout.Token;
        // LinkStateLost retains the underlying AMQP exception, which MessageLockLost omits.
        using var sdkLogs = AzureEventSourceListener.CreateConsoleLogger(EventLevel.Warning);
        await using var network = new NetworkBuilder().Build();
        var databaseBuilder = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
            .WithNetwork(network)
            .WithNetworkAliases(ServiceBusBuilder.DatabaseNetworkAlias);
        var emulatorBuilder = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:1.1.2")
            .WithAcceptLicenseAgreement(true)
            .WithConfig(Path.Combine(AppContext.BaseDirectory, "servicebus-config.json"));
        if (OperatingSystem.IsLinux())
        {
            // Keep every published dependency port outside Linux's outbound ephemeral range,
            // matching the Kafka fixture's mitigation for moby/moby#54079.
            databaseBuilder = databaseBuilder.WithPortBinding("20000-29999", "1433");
            emulatorBuilder = emulatorBuilder.WithPortBinding("20000-29999", "5672")
                .WithPortBinding("20000-29999", "5300");
        }
        await using var database = databaseBuilder.Build();
        await using var emulator = emulatorBuilder
            .WithMsSqlContainer(network, database, ServiceBusBuilder.DatabaseNetworkAlias).Build();
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
            var outgoing = new ServiceBusMessage(new BinaryData(new byte[] { 42 }))
            {
                MessageId = operation,
                ContentType = "application/cbor",
            };
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
                await Assert.That(context.ServiceBus().Message.ContentType).IsEqualTo(outgoing.ContentType);
                await Assert.That(context.ServiceBus().Message.ApplicationProperties.ContainsKey("content-type")).IsFalse();
                await Assert.That(context.Headers.TryGetValue("content-type", out var contentType)).IsTrue();
                await Assert.That(contentType.Text).IsEqualTo(outgoing.ContentType);
                await Assert.That(ServiceBusBinding.GetBody(context.ServiceBus().Callback).First.Span[0]).IsEqualTo((byte)42);
                await Settle(context.ServiceBus(), operation, properties);
            };
            var args = new ProcessMessageEventArgs(incoming, receiver, "emulator", token);
            if (nativeOnly)
            {
                // Run the same sequence without invoking the adapter or pipeline.
                switch (operation)
                {
                    case "complete": await receiver.CompleteMessageAsync(incoming, token); break;
                    case "abandon": await receiver.AbandonMessageAsync(incoming, properties, token); break;
                    case "defer": await receiver.DeferMessageAsync(incoming, properties, token); break;
                    case "dead_letter": await receiver.DeadLetterMessageAsync(incoming, properties, "poison", "details", token); break;
                }
            }
            else
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
                var resultReceiver = operation == "dead_letter" ? deadLetters : receiver;
                try { await resultReceiver.CompleteMessageAsync(result, token); }
                catch
                {
                    Console.Error.WriteLine($"Final completion after {operation} failed: nativeOnly={nativeOnly}, now={DateTimeOffset.UtcNow:O}");
                    Console.Error.WriteLine($"Original: {Describe(incoming, receiver)}");
                    Console.Error.WriteLine($"Result: {Describe(result, resultReceiver)}");
                    try
                    {
                        var logs = await emulator.GetLogsAsync(ct: token);
                        Console.Error.WriteLine(logs.Stdout);
                        Console.Error.WriteLine(logs.Stderr);
                    }
                    catch (Exception loggingError)
                    {
                        Console.Error.WriteLine($"Could not read emulator logs: {loggingError}");
                    }
                    throw;
                }
            }
        }
        await Assert.That(await receiver.PeekMessageAsync(cancellationToken: token)).IsNull();
        await Assert.That(await deadLetters.PeekMessageAsync(cancellationToken: token)).IsNull();
    }

    private static string Describe(ServiceBusReceivedMessage message, ServiceBusReceiver receiver) =>
        $"receiver={receiver.Identifier}, entity={receiver.EntityPath}, mode={receiver.ReceiveMode}, message={message.MessageId}, " +
        $"sequence={message.SequenceNumber}, delivery={message.DeliveryCount}, lock={message.LockToken}, " +
        $"lockedUntil={message.LockedUntil:O}, state={message.State}";
}
