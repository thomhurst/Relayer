using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class ServiceBusDeadLetterTests
{
    [Test]
    public async Task Named_registration_lists_queue_and_subscription_sources_without_owning_client()
    {
        var client = new Client();
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.ServiceBus("bus", bus =>
        {
            bus.Options.Configure(o =>
            {
                o.Client = client;
                o.Queues.Add(new("orders"));
                o.Topics.Add(new("events"));
                o.Subscriptions.Add(new("events", "audit"));
            });
            bus.Consume("orders", c => c.Handler<ServiceBusReceiveEmulatorTests.EmulatorHandler>());
            bus.Consume("events", c =>
            {
                c.Options.Subscription = "audit";
                c.Handler<ServiceBusReceiveEmulatorTests.EmulatorHandler>();
            });
        }));

        await using (var provider = services.BuildServiceProvider())
        {
            var manager = provider.GetRequiredKeyedService<IDeadLetterManager>("bus");
            await Assert.That(manager.List().Select(s => s.Id).ToArray()).IsEquivalentTo(new[] { "orders", "events/Subscriptions/audit" });
            await manager.PeekAsync("events/Subscriptions/audit");
            await Assert.That(client.Subscription).IsEqualTo("audit");
            await Assert.That(client.Options!.SubQueue).IsEqualTo(SubQueue.DeadLetter);
            await Assert.That(client.Options.ReceiveMode).IsEqualTo(ServiceBusReceiveMode.PeekLock);
        }

        await Assert.That(client.Disposed).IsFalse();
    }

    [Test]
    public async Task Peek_is_non_destructive_and_uses_native_sequence_cursor_and_owned_snapshot()
    {
        var client = new Client();
        var message = Message("first", 42);
        client.Receiver.Messages = [message];
        var manager = Manager(client);
        var page = await manager.PeekAsync("orders", 3, "12");
        await Assert.That(client.Receiver.From).IsEqualTo(12L);
        await Assert.That(page.NextCursor).IsEqualTo("43");
        await Assert.That(page.Messages[0].Features.GetRequired<ServiceBusReceivedMessage>()).IsSameReferenceAs(message);
        await Assert.That(page.Messages[0].Body.ToArray()).IsEquivalentTo(new byte[] { 1, 2 });
        await Assert.That(client.Events.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Peek_includes_native_value_and_sequence_messages_with_data_messages()
    {
        var client = new Client();
        var value = NativeMessage("value", AmqpMessageBody.FromValue("poison"));
        var sequence = NativeMessage("sequence", AmqpMessageBody.FromSequence([new List<object> { "poison" }]));
        var empty = NativeMessage("empty", AmqpMessageBody.FromData([ReadOnlyMemory<byte>.Empty]));
        var data = NativeMessage("data", AmqpMessageBody.FromData([new byte[] { 1 }, new byte[] { 2 }]));
        client.Receiver.Messages = [value, sequence, empty, data, Message("last", 42)];

        var page = await Manager(client).PeekAsync("orders");

        await Assert.That(page.Messages.Select(message => message.MessageId))
            .IsEquivalentTo(new string?[] { "value", "sequence", "empty", "data", "last" });
        await Assert.That(page.Messages[0].Features.GetRequired<ServiceBusReceivedMessage>()).IsSameReferenceAs(value);
        await Assert.That(page.Messages[1].Features.GetRequired<ServiceBusReceivedMessage>()).IsSameReferenceAs(sequence);
        await Assert.That(page.Messages[0].HasByteBody).IsFalse();
        await Assert.That(page.Messages[0].Body.IsEmpty).IsTrue();
        await Assert.That(page.Messages[1].HasByteBody).IsFalse();
        await Assert.That(page.Messages[1].Body.IsEmpty).IsTrue();
        await Assert.That(page.Messages[2].HasByteBody).IsTrue();
        await Assert.That(page.Messages[2].Body.IsEmpty).IsTrue();
        await Assert.That(page.Messages[3].HasByteBody).IsTrue();
        await Assert.That(page.Messages[3].Body.ToArray()).IsEquivalentTo(new byte[] { 1, 2 });
        await Assert.That(page.NextCursor).IsEqualTo("43");
    }

    [Test]
    public async Task Filtered_redrive_can_select_data_after_value_and_sequence()
    {
        var client = new Client();
        var value = NativeMessage("value", AmqpMessageBody.FromValue("poison"));
        var sequence = NativeMessage("sequence", AmqpMessageBody.FromSequence([new List<object> { "poison" }]));
        client.Receiver.Messages = [value, sequence, Message("selected", 3)];

        var count = await Manager(client).RedriveAsync("orders", message => message.MessageId == "selected", 3);

        await Assert.That(count).IsEqualTo(1);
        await Assert.That(client.Events.SequenceEqual(["abandon:value", "abandon:sequence", "send:selected", "complete:selected"])).IsTrue();
    }

    [Test]
    public async Task Failed_peek_preserves_error_when_receiver_disposal_fails()
    {
        var client = new Client();
        client.Receiver.PeekFailure = new InvalidOperationException("peek");
        client.Receiver.DisposeFailure = new InvalidOperationException("receiver close");

        await Assert.That(async () => await Manager(client).PeekAsync("orders"))
            .Throws<InvalidOperationException>().WithMessage("peek");
        await Assert.That(client.Receiver.Disposals).IsEqualTo(1);
    }

    [Test]
    public async Task Filtered_redrive_sends_before_complete_and_preserves_native_properties()
    {
        var client = new Client();
        client.Receiver.Messages = [Message("skip", 1), Message("copy", 2)];
        var count = await Manager(client).RedriveAsync("orders", m => m.MessageId == "copy", 2);
        await Assert.That(count).IsEqualTo(1);
        await Assert.That(client.Events.SequenceEqual(new[] { "abandon:skip", "send:copy", "complete:copy" })).IsTrue();
        var sent = client.Sender.Sent!;
        await Assert.That(sent.Body.ToArray()).IsEquivalentTo(new byte[] { 1, 2 });
        await Assert.That(sent.SessionId).IsEqualTo("session");
        await Assert.That(sent.CorrelationId).IsEqualTo("correlation");
        await Assert.That(sent.ApplicationProperties["traceparent"]).IsEqualTo("creation");
        await Assert.That(sent.GetRawAmqpMessage().Properties.AbsoluteExpiryTime).IsNull();
    }

    [Test]
    public async Task Failed_send_and_throwing_filter_do_not_complete_original()
    {
        var client = new Client();
        client.Receiver.Messages = [Message("copy", 1)];
        client.Sender.Fail = true;
        await Assert.That(async () => await Manager(client).RedriveAsync("orders")).Throws<InvalidOperationException>();
        await Assert.That(client.Events.ToArray()).IsEquivalentTo(new[] { "send:copy", "abandon:copy" });
        client.Events.Clear();
        await Assert.That(async () => await Manager(client).RedriveAsync("orders", _ => throw new InvalidOperationException())).Throws<InvalidOperationException>();
        await Assert.That(client.Events.ToArray()).IsEquivalentTo(new[] { "abandon:copy" });
        await Assert.That(client.Receiver.Disposals).IsEqualTo(2);
    }

    [Test]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task Failed_send_preserves_error_when_native_link_disposal_fails(bool senderFails, bool receiverFails)
    {
        var client = new Client();
        client.Receiver.Messages = [Message("copy", 1), Message("remaining", 2)];
        client.Sender.Fail = true;
        client.Sender.DisposeFailure = senderFails ? new InvalidOperationException("sender close") : null;
        client.Receiver.DisposeFailure = receiverFails ? new InvalidOperationException("receiver close") : null;

        await Assert.That(async () => await Manager(client).RedriveAsync("orders"))
            .Throws<InvalidOperationException>().WithMessage("send");
        await Assert.That(client.Events).IsEquivalentTo(new[] { "send:copy", "abandon:copy", "abandon:remaining" });
        await Assert.That(client.Sender.Disposals).IsEqualTo(1);
        await Assert.That(client.Receiver.Disposals).IsEqualTo(1);
    }

    [Test]
    [Arguments(false, "receiver close")]
    [Arguments(true, "sender close")]
    public async Task Successful_redrive_propagates_first_close_error_and_disposes_both_links(bool senderFails, string error)
    {
        var client = new Client();
        client.Receiver.Messages = [Message("copy", 1)];
        client.Sender.DisposeFailure = senderFails ? new InvalidOperationException("sender close") : null;
        client.Receiver.DisposeFailure = new InvalidOperationException("receiver close");

        await Assert.That(async () => await Manager(client).RedriveAsync("orders"))
            .Throws<InvalidOperationException>().WithMessage(error);
        await Assert.That(client.Events).IsEquivalentTo(new[] { "send:copy", "complete:copy" });
        await Assert.That(client.Sender.Disposals).IsEqualTo(1);
        await Assert.That(client.Receiver.Disposals).IsEqualTo(1);
    }

    [Test]
    public async Task Throwing_filter_releases_failing_delivery_and_unprocessed_suffix()
    {
        var client = new Client();
        client.Receiver.Messages = [Message("rejected", 1), Message("failing", 2), Message("suffix", 3)];
        await Assert.That(async () => await Manager(client).RedriveAsync("orders", message =>
        {
            if (message.MessageId == "failing")
            {
                throw new InvalidOperationException("filter failure");
            }

            return false;
        }, 3)).Throws<InvalidOperationException>();
        await Assert.That(client.Events.ToArray()).IsEquivalentTo(new[]
        { "abandon:rejected", "abandon:failing", "abandon:suffix" });
        await Assert.That(client.Receiver.Disposals).IsEqualTo(1);
    }

    [Test]
    public async Task Purge_only_completes_bounded_native_batch_and_rejects_invalid_requests()
    {
        var client = new Client();
        client.Receiver.Messages = [Message("delete", 1)];
        var manager = Manager(client);
        await Assert.That(await manager.PurgeAsync("orders", 7)).IsEqualTo(1);
        await Assert.That(client.Receiver.Maximum).IsEqualTo(7);
        await Assert.That(client.Events.ToArray()).IsEquivalentTo(new[] { "complete:delete" });
        await Assert.That(async () => await manager.PurgeAsync("other")).Throws<ArgumentException>();
        await Assert.That(async () => await manager.PurgeAsync("orders", 101)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await manager.PeekAsync("orders", cursor: "-1")).Throws<ArgumentException>();
    }

    [Test]
    public async Task Cancellation_reaches_native_receive_without_sending_or_settling()
    {
        var client = new Client();
        client.Receiver.DisposeFailure = new InvalidOperationException("receiver close");
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.That(async () => await Manager(client).RedriveAsync("orders", cancellationToken: cancellation.Token))
            .Throws<OperationCanceledException>();
        await Assert.That(client.Events.Count).IsEqualTo(0);
        await Assert.That(client.Receiver.Disposals).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Mid_batch_failure_releases_suffix_and_preserves_error_when_cleanup_fails(bool purge)
    {
        var client = new Client();
        client.Receiver.Messages = [Message("done", 1), Message("fail", 2), Message("remaining", 3)];
        client.Receiver.FailComplete = "fail";
        client.Receiver.FailAbandon = "fail";
        var manager = Manager(client);
        await Assert.That(async () =>
        {
            if (purge)
            {
                await manager.PurgeAsync("orders", 3);
            }
            else
            {
                await manager.RedriveAsync("orders", maxMessages: 3);
            }
        }).Throws<InvalidOperationException>().WithMessage("complete");
        await Assert.That(client.Events.TakeLast(2).ToArray())
            .IsEquivalentTo(new[] { "abandon:fail", "abandon:remaining" });
        await Assert.That(client.Events.Contains("abandon:done")).IsFalse();
    }

    [Test]
    public async Task Cancellation_after_receive_releases_entire_batch_with_uncancelled_token()
    {
        var client = new Client();
        client.Receiver.Messages = [Message("first", 1), Message("second", 2)];
        using var cancellation = new CancellationTokenSource();
        await Assert.That(async () => await Manager(client).RedriveAsync("orders", _ =>
        {
            cancellation.Cancel();
            cancellation.Token.ThrowIfCancellationRequested();
            return true;
        }, 2, cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(client.Events.ToArray()).IsEquivalentTo(new[] { "abandon:first", "abandon:second" });
    }

    private static ServiceBusDeadLetterManager Manager(Client client)
    {
        return new(client,
                [new ConsumerDefinition<ServiceBusConsumerOptions>("orders", new(), [], 1, 1, TimeSpan.Zero)]);
    }

    private static ServiceBusReceivedMessage NativeMessage(string id, AmqpMessageBody body)
    {
        var message = new AmqpAnnotatedMessage(body);
        message.Properties.MessageId = new AmqpMessageId(id);
        return ServiceBusReceivedMessage.FromAmqpMessage(message, new BinaryData(Guid.NewGuid().ToByteArray()));
    }

    private static ServiceBusReceivedMessage Message(string id, long sequence)
    {
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: new BinaryData(new byte[] { 1, 2 }), messageId: id, sessionId: "session", correlationId: "correlation",
            properties: new Dictionary<string, object> { ["traceparent"] = "creation" }, sequenceNumber: sequence);
        message.GetRawAmqpMessage().Properties.AbsoluteExpiryTime = DateTimeOffset.UnixEpoch;
        message.GetRawAmqpMessage().Properties.CreationTime = DateTimeOffset.UnixEpoch;
        return message;
    }

    private sealed class Client : ServiceBusClient
    {
        internal readonly List<string> Events = [];

        internal Receiver Receiver
        {
            get;
        }

        internal Sender Sender
        {
            get;
        }
        internal ServiceBusReceiverOptions? Options;
        internal string? Subscription;
        internal bool Disposed;

        internal Client()
        {
            Receiver = new(Events);
            Sender = new(Events);
        }

        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions? options = null)
        {
            Options = options;
            return Receiver;
        }

        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions? options = null)
        {
            Subscription = subscriptionName;
            Options = options;
            return Receiver;
        }

        public override ServiceBusSender CreateSender(string queueOrTopicName)
        {
            return Sender;
        }

        public override ValueTask DisposeAsync()
        {
            Disposed = true;
            return default;
        }
    }

    private sealed class Receiver(List<string> events) : ServiceBusReceiver
    {
        internal IReadOnlyList<ServiceBusReceivedMessage> Messages = [];
        internal long? From;
        internal int Maximum;
        internal int Disposals;
        internal string? FailComplete;
        internal string? FailAbandon;
        internal Exception? PeekFailure;
        internal Exception? DisposeFailure;

        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null, CancellationToken cancellationToken = default)
        {
            From = fromSequenceNumber;
            return PeekFailure is { } failure ? Task.FromException<IReadOnlyList<ServiceBusReceivedMessage>>(failure) : Task.FromResult(Messages);
        }

        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Maximum = maxMessages;
            return Task.FromResult(Messages);
        }

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            events.Add("complete:" + message.MessageId);
            return message.MessageId == FailComplete ? Task.FromException(new InvalidOperationException("complete")) : Task.CompletedTask;
        }

        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            events.Add("abandon:" + message.MessageId);
            return message.MessageId == FailAbandon ? Task.FromException(new InvalidOperationException("abandon")) : Task.CompletedTask;
        }

        public override ValueTask DisposeAsync()
        {
            Disposals++;
            return DisposeFailure is { } failure ? ValueTask.FromException(failure) : default;
        }
    }

    private sealed class Sender(List<string> events) : ServiceBusSender
    {
        internal ServiceBusMessage? Sent;
        internal bool Fail;
        internal int Disposals;
        internal Exception? DisposeFailure;

        public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
        {
            Sent = message;
            events.Add("send:" + message.MessageId);
            return Fail ? Task.FromException(new InvalidOperationException("send")) : Task.CompletedTask;
        }

        public override ValueTask DisposeAsync()
        {
            Disposals++;
            return DisposeFailure is { } failure ? ValueTask.FromException(failure) : default;
        }
    }
}
