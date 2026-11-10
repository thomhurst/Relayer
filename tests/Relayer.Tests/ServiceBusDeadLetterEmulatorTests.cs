using Azure.Messaging.ServiceBus;
using Relayer.AzureServiceBus;

namespace Relayer.Tests;

public sealed class ServiceBusDeadLetterEmulatorTests
{
    [Test]
    public async Task Queue_subscription_and_session_DLQs_support_peek_redrive_and_purge()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = timeout.Token;
        await using var broker = await ServiceBusReceiveBroker.StartAsync(["dlq-orders"], ["dlq-sessions"], includeTopic: true);
        await using var client = new ServiceBusClient(broker.ConnectionString);
        foreach (var (entity, subscription, session) in new[]
        {
            ("dlq-orders", (string?)null, (string?)null),
            ("receive-events", "audit", (string?)null),
            ("dlq-sessions", (string?)null, "customer"),
        })
        {
            var manager = new ServiceBusDeadLetterManager(client,
                [new ConsumerDefinition<ServiceBusConsumerOptions>(entity, new() { Subscription = subscription }, [], 1, 1, TimeSpan.Zero)]);
            var source = manager.List()[0].Id;
            await using var sender = client.CreateSender(entity);
            for (var i = 0; i < 3; i++)
            {
                var outgoing = new ServiceBusMessage(new BinaryData(new byte[] { (byte)i }))
                { MessageId = "message-" + i, SessionId = session, CorrelationId = "correlation" };
                outgoing.ApplicationProperties["traceparent"] = "creation-context";
                await sender.SendMessageAsync(outgoing, token);
            }
            await using ServiceBusReceiver original = session is not null
                ? await client.AcceptSessionAsync(entity, session, cancellationToken: token)
                : subscription is not null ? client.CreateReceiver(entity, subscription) : client.CreateReceiver(entity);
            for (var i = 0; i < 3; i++)
            {
                var message = await original.ReceiveMessageAsync(TimeSpan.FromSeconds(10), token)
                    ?? throw new InvalidOperationException("Expected original delivery.");
                await original.DeadLetterMessageAsync(message, "poison", "test failure", token);
            }
            var first = await manager.PeekAsync(source, 1, cancellationToken: token);
            await Assert.That(first.Messages.Count).IsEqualTo(1);
            var next = await manager.PeekAsync(source, 10, first.NextCursor, token);
            await Assert.That(next.Messages.Count).IsEqualTo(2);
            // Native receives may return fewer than maxMessages, even with three messages queued.
            // Fail on the first delivery so cleanup is exercised for any nonempty batch.
            await Assert.That(async () => await manager.RedriveAsync(source,
                _ => throw new InvalidOperationException("filter failure"), 3, token))
                .Throws<InvalidOperationException>();
            // A new native receiver must immediately see the entire abandoned batch.
            await using (var retry = subscription is not null
                ? client.CreateReceiver(entity, subscription, new() { SubQueue = SubQueue.DeadLetter })
                : client.CreateReceiver(entity, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }))
            {
                var availableMessages = new List<ServiceBusReceivedMessage>();
                for (var i = 0; i < 3; i++)
                {
                    var available = await retry.ReceiveMessageAsync(TimeSpan.FromSeconds(1), token)
                        ?? throw new InvalidOperationException("Failed maintenance left a delivery locked.");
                    availableMessages.Add(available);
                }
                foreach (var available in availableMessages)
                    await retry.AbandonMessageAsync(available, cancellationToken: token);
            }
            var selected = first.Messages[0].MessageId;
            var count = await manager.RedriveAsync(source, m => m.MessageId == selected, 3, token);
            await Assert.That(count).IsEqualTo(1);
            var redriven = await original.ReceiveMessageAsync(TimeSpan.FromSeconds(10), token)
                ?? throw new InvalidOperationException("Expected redriven delivery.");
            await Assert.That(redriven.MessageId).IsEqualTo(selected);
            await Assert.That(redriven.CorrelationId).IsEqualTo("correlation");
            await Assert.That(redriven.SessionId).IsEqualTo(session);
            await Assert.That(redriven.ApplicationProperties["traceparent"]).IsEqualTo("creation-context");
            await original.CompleteMessageAsync(redriven, token);
            await Assert.That(await manager.PurgeAsync(source, 1, token)).IsEqualTo(1);
            await Assert.That((await manager.PeekAsync(source, cancellationToken: token)).Messages.Count).IsEqualTo(1);
            await Assert.That(await manager.PurgeAsync(source, 10, token)).IsEqualTo(1);
            await Assert.That((await manager.PeekAsync(source, cancellationToken: token)).Messages.Count).IsEqualTo(0);
        }
    }
}
