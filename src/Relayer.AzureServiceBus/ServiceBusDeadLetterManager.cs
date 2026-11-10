using System.Globalization;
using Azure.Messaging.ServiceBus;

namespace Relayer.AzureServiceBus;

internal sealed class ServiceBusDeadLetterManager : IDeadLetterManager
{
    private readonly ServiceBusClient _client;
    private readonly Dictionary<string, Source> _sources = new(StringComparer.Ordinal);
    private readonly IReadOnlyList<DeadLetterSource> _listed;

    internal ServiceBusDeadLetterManager(ServiceBusClient client, IReadOnlyList<IConsumerDefinition> consumers)
    {
        _client = client;
        foreach (var declaration in consumers)
        {
            var consumer = (ConsumerDefinition<ServiceBusConsumerOptions>)declaration;
            var subscription = consumer.Options.Subscription;
            var id = subscription is null ? consumer.Source : consumer.Source + "/Subscriptions/" + subscription;
            _sources.TryAdd(id, new(consumer.Source, subscription));
        }
        _listed = Array.AsReadOnly(_sources.Select(item => new DeadLetterSource(item.Key, item.Value.Entity)).ToArray());
    }

    public IReadOnlyList<DeadLetterSource> List() => _listed;

    public async ValueTask<DeadLetterPage> PeekAsync(string source, int maxMessages = 20, string? cursor = null,
        CancellationToken cancellationToken = default)
    {
        var endpoint = GetSource(source, maxMessages);
        long? sequence = null;
        if (cursor is not null)
        {
            if (!long.TryParse(cursor, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
                throw new ArgumentException("Service Bus peek cursor must be a non-negative sequence number.", nameof(cursor));
            sequence = parsed;
        }
        await using var receiver = CreateReceiver(endpoint);
        var messages = await receiver.PeekMessagesAsync(maxMessages, sequence, cancellationToken).ConfigureAwait(false);
        var result = new DeadLetterMessage[messages.Count];
        for (var i = 0; i < result.Length; i++) result[i] = Snapshot(messages[i]);
        var next = messages.Count == 0 || messages[^1].SequenceNumber == long.MaxValue ? null
            : (messages[^1].SequenceNumber + 1).ToString(CultureInfo.InvariantCulture);
        return new(Array.AsReadOnly(result), next);
    }

    public ValueTask<int> RedriveAsync(string source, Func<DeadLetterMessage, bool>? filter = null,
        int maxMessages = 20, CancellationToken cancellationToken = default) =>
        MaintainAsync(source, filter, maxMessages, false, cancellationToken);

    public ValueTask<int> PurgeAsync(string source, int maxMessages = 20, CancellationToken cancellationToken = default) =>
        MaintainAsync(source, null, maxMessages, true, cancellationToken);

    private async ValueTask<int> MaintainAsync(string source, Func<DeadLetterMessage, bool>? filter,
        int maxMessages, bool purge, CancellationToken cancellationToken)
    {
        var endpoint = GetSource(source, maxMessages);
        await using var receiver = CreateReceiver(endpoint);
        // Receive one bounded native batch; filtering cannot repeatedly receive an unmatched delivery.
        // Unsettled originals remain in the DLQ if sending, filtering or settlement fails.
        var messages = await receiver.ReceiveMessagesAsync(maxMessages, TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
        var next = 0;
        ServiceBusSender? sender = null;
        try
        {
            var changed = 0;
            for (; next < messages.Count; next++)
            {
                var message = messages[next];
                cancellationToken.ThrowIfCancellationRequested();
                if (!purge)
                {
                    if (filter is not null && !filter(Snapshot(message)))
                    {
                        await receiver.AbandonMessageAsync(message, cancellationToken: cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                    sender ??= _client.CreateSender(endpoint.Entity);
                    // Native copy preserves payload, application properties, identity, session and partition keys.
                    var outgoing = new ServiceBusMessage(message);
                    // A redrive begins a new broker lifetime, using the original TTL rather than expired timestamps.
                    outgoing.GetRawAmqpMessage().Properties.AbsoluteExpiryTime = null;
                    outgoing.GetRawAmqpMessage().Properties.CreationTime = null;
                    await sender.SendMessageAsync(outgoing, cancellationToken).ConfigureAwait(false);
                }
                await receiver.CompleteMessageAsync(message, cancellationToken).ConfigureAwait(false);
                changed++;
            }
            return changed;
        }
        catch
        {
            // Release the failing delivery and unprocessed suffix, even after caller cancellation.
            // Preserve the original failure if a lock expired or the broker cannot accept cleanup.
            for (; next < messages.Count; next++)
            {
                try
                {
                    await receiver.AbandonMessageAsync(messages[next], cancellationToken: CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception) { }
            }
            throw;
        }
        finally
        {
            if (sender is not null) await sender.DisposeAsync().ConfigureAwait(false);
        }
    }

    private Source GetSource(string source, int maxMessages)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxMessages, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxMessages, 100);
        ArgumentNullException.ThrowIfNull(source);
        return _sources.TryGetValue(source, out var endpoint) ? endpoint
            : throw new ArgumentException($"Unknown Service Bus dead-letter source '{source}'. Use List() to find configured sources.", nameof(source));
    }

    private ServiceBusReceiver CreateReceiver(Source source)
    {
        var options = new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.PeekLock };
        return source.Subscription is { } subscription
            ? _client.CreateReceiver(source.Entity, subscription, options)
            : _client.CreateReceiver(source.Entity, options);
    }

    private static DeadLetterMessage Snapshot(ServiceBusReceivedMessage message)
    {
        var result = new DeadLetterMessage { MessageId = message.MessageId, Body = message.Body.ToArray() };
        result.Features.Set(message);
        return result;
    }

    private sealed record Source(string Entity, string? Subscription);
}
