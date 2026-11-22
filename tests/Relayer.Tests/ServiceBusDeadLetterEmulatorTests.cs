using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Relayer.AzureServiceBus;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class ServiceBusDeadLetterEmulatorTests
{
    [Test]
    [Arguments(AmqpMessageBodyType.Value)]
    [Arguments(AmqpMessageBodyType.Sequence)]
    public async Task Native_body_is_available_for_inspection_and_preserved_by_filtered_redrive(AmqpMessageBodyType bodyType)
    {
        var queue = TestContext.Current!.Isolation.GetIsolatedName("dlq-native-body");
        await using var broker = await ServiceBusReceiveBroker.StartAsync([queue]);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var client = new ServiceBusClient(broker.ConnectionString);
        await using var sender = client.CreateSender(queue);
        await using var receiver = client.CreateReceiver(queue);
        var body = bodyType == AmqpMessageBodyType.Value
            ? AmqpMessageBody.FromValue("poison")
            : AmqpMessageBody.FromSequence([new List<object> { "poison" }]);
        await sender.SendMessageAsync(new ServiceBusMessage(new AmqpAnnotatedMessage(body)) { MessageId = "native" }, token);
        var original = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), token)
            ?? throw new InvalidOperationException("Expected original delivery.");
        await receiver.DeadLetterMessageAsync(original, "poison", cancellationToken: token);

        var manager = new ServiceBusDeadLetterManager(client,
            [new ConsumerDefinition<ServiceBusConsumerOptions>(queue, new(), [], 1, 1, TimeSpan.Zero)]);
        var page = await manager.PeekAsync(queue, cancellationToken: token);

        var snapshot = page.Messages.Single();
        await Assert.That(snapshot.MessageId).IsEqualTo("native");
        await Assert.That(snapshot.HasByteBody).IsFalse();
        await Assert.That(snapshot.Body.IsEmpty).IsTrue();
        var native = snapshot.Features.GetRequired<ServiceBusReceivedMessage>();
        await Assert.That(native.DeadLetterReason).IsEqualTo("poison");
        await Assert.That(native.GetRawAmqpMessage().Body.BodyType).IsEqualTo(bodyType);

        var count = await manager.RedriveAsync(queue, message => message.MessageId == "native", cancellationToken: token);
        await Assert.That(count).IsEqualTo(1);
        var redriven = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), token)
            ?? throw new InvalidOperationException("Expected redriven delivery.");
        await Assert.That(redriven.MessageId).IsEqualTo("native");
        var redrivenBody = redriven.GetRawAmqpMessage().Body;
        await Assert.That(redrivenBody.BodyType).IsEqualTo(bodyType);

        if (bodyType == AmqpMessageBodyType.Value)
        {
            await Assert.That(redrivenBody.TryGetValue(out var value) && Equals(value, "poison")).IsTrue();
        }
        else
        {
            await Assert.That(redrivenBody.TryGetSequence(out var sequence) && Equals(sequence?.Single().Single(), "poison")).IsTrue();
        }

        await Assert.That((await manager.PeekAsync(queue, cancellationToken: token)).Messages.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Queue_subscription_and_session_DLQs_support_peek_redrive_and_purge()
    {
        await using var broker = await ServiceBusReceiveBroker.StartAsync(["dlq-orders"], ["dlq-sessions"], includeTopic: true);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = timeout.Token;
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
                {
                    MessageId = "message-" + i,
                    SessionId = session,
                    CorrelationId = "correlation"
                };
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
                ? client.CreateReceiver(entity, subscription, new()
                {
                    SubQueue = SubQueue.DeadLetter
                })
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
                {
                    await retry.AbandonMessageAsync(available, cancellationToken: token);
                }
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
