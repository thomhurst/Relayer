using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace Relayer.AzureServiceBus;

internal sealed class ServiceBusTopology(string name, ServiceBusClient client,
    AzureServiceBusTransportOptions options, ServiceBusAdministrationClient? administration)
{
    internal async ValueTask<TopologyReport> ApplyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (mode == TopologyMode.None) return new([]);
        if (mode is not (TopologyMode.Validate or TopologyMode.Provision))
            throw new ArgumentOutOfRangeException(nameof(mode));
        var differences = new List<string>();
        foreach (var queue in options.Queues)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (mode == TopologyMode.Validate)
            {
                await ValidateSendAsync(queue.Name, differences, cancellationToken).ConfigureAwait(false);
                if (queue.RequiresSession) UnresolvedSessions(queue.Name, differences);
            }
            else await ProvisionQueueAsync(queue, differences, cancellationToken).ConfigureAwait(false);
        }
        foreach (var topic in options.Topics)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (mode == TopologyMode.Validate)
                await ValidateSendAsync(topic.Name, differences, cancellationToken).ConfigureAwait(false);
            else await ProvisionTopicAsync(topic, differences, cancellationToken).ConfigureAwait(false);
        }
        foreach (var subscription in options.Subscriptions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (mode == TopologyMode.Validate)
            {
                var entity = subscription.TopicName + "/Subscriptions/" + subscription.SubscriptionName;
                var receiver = client.CreateReceiver(subscription.TopicName, subscription.SubscriptionName);
                await using var receiverLifetime = receiver.ConfigureAwait(false);
                try
                {
                    // Peek is read-only: no message is sent, locked, received or settled.
                    await receiver.PeekMessageAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                catch (ServiceBusException error) when (error.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
                {
                    differences.Add($"Missing subscription '{entity}'. Provision it outside the application or use Provision explicitly.");
                }
                if (subscription.RequiresSession) UnresolvedSessions(entity, differences);
            }
            else await ProvisionSubscriptionAsync(subscription, differences, cancellationToken).ConfigureAwait(false);
        }
        return new(differences);
    }

    private static void UnresolvedSessions(string entity, List<string> differences) =>
        differences.Add($"Entity '{entity}': RequiresSession=true cannot be verified with least-privilege link access. Use an explicit Provision startup with administration credentials to verify native settings. Validate never requests Manage permission.");

    private async Task ValidateSendAsync(string entity, List<string> differences, CancellationToken cancellationToken)
    {
        var sender = client.CreateSender(entity);
        await using var senderLifetime = sender.ConfigureAwait(false);
        try
        {
            // Opens an authenticated send link and discovers its byte limit, without a probe message.
            using var batch = await sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ServiceBusException error) when (error.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            differences.Add($"Missing send entity '{entity}'. Provision it outside the application or use Provision explicitly.");
        }
    }

    private ServiceBusAdministrationClient Administration => administration ??
        throw new InvalidOperationException($"Service Bus transport '{name}': Provision requires AdministrationClient or an owned connection-string client.");

    private static bool IsMissing(Exception error) => error is RequestFailedException { Status: 404 } or
        ServiceBusException { Reason: ServiceBusFailureReason.MessagingEntityNotFound };

    private static bool IsAlreadyPresent(Exception error) => error is RequestFailedException { Status: 409 } or
        ServiceBusException { Reason: ServiceBusFailureReason.MessagingEntityAlreadyExists };

    private async Task ProvisionQueueAsync(CreateQueueOptions expected, List<string> differences, CancellationToken cancellationToken)
    {
        QueueProperties actual;
        try { actual = (await Administration.GetQueueAsync(expected.Name, cancellationToken).ConfigureAwait(false)).Value; }
        catch (Exception error) when (IsMissing(error))
        {
            try { actual = (await Administration.CreateQueueAsync(expected, cancellationToken).ConfigureAwait(false)).Value; }
            catch (Exception race) when (IsAlreadyPresent(race))
            {
                actual = (await Administration.GetQueueAsync(expected.Name, cancellationToken).ConfigureAwait(false)).Value;
            }
        }
        Compare(expected, new CreateQueueOptions(actual), differences);
    }

    private async Task ProvisionTopicAsync(CreateTopicOptions expected, List<string> differences, CancellationToken cancellationToken)
    {
        TopicProperties actual;
        try { actual = (await Administration.GetTopicAsync(expected.Name, cancellationToken).ConfigureAwait(false)).Value; }
        catch (Exception error) when (IsMissing(error))
        {
            try { actual = (await Administration.CreateTopicAsync(expected, cancellationToken).ConfigureAwait(false)).Value; }
            catch (Exception race) when (IsAlreadyPresent(race))
            {
                actual = (await Administration.GetTopicAsync(expected.Name, cancellationToken).ConfigureAwait(false)).Value;
            }
        }
        Compare(expected, new CreateTopicOptions(actual), differences);
    }

    private async Task ProvisionSubscriptionAsync(CreateSubscriptionOptions expected, List<string> differences, CancellationToken cancellationToken)
    {
        SubscriptionProperties actual;
        try { actual = (await Administration.GetSubscriptionAsync(expected.TopicName, expected.SubscriptionName, cancellationToken).ConfigureAwait(false)).Value; }
        catch (Exception error) when (IsMissing(error))
        {
            try { actual = (await Administration.CreateSubscriptionAsync(expected, cancellationToken).ConfigureAwait(false)).Value; }
            catch (Exception race) when (IsAlreadyPresent(race))
            {
                actual = (await Administration.GetSubscriptionAsync(expected.TopicName, expected.SubscriptionName, cancellationToken).ConfigureAwait(false)).Value;
            }
        }
        Compare(expected, new CreateSubscriptionOptions(actual), differences);
    }

    private static void Difference<T>(string entity, string property, T expected, T actual, List<string> differences)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
            differences.Add($"Entity '{entity}': {property} expected '{expected}', actual '{actual}'. Reconcile it through infrastructure configuration; Relayer never deletes or modifies existing entities.");
    }

    private static void Difference(string entity, string property, string? expected, string? actual, List<string> differences) =>
        Difference<string>(entity, property, expected ?? string.Empty, actual ?? string.Empty, differences);

    private static void Compare(CreateQueueOptions expected, CreateQueueOptions actual, List<string> differences)
    {
        var entity = expected.Name;
        Difference(entity, nameof(expected.AutoDeleteOnIdle), expected.AutoDeleteOnIdle, actual.AutoDeleteOnIdle, differences);
        Difference(entity, nameof(expected.DeadLetteringOnMessageExpiration), expected.DeadLetteringOnMessageExpiration, actual.DeadLetteringOnMessageExpiration, differences);
        Difference(entity, nameof(expected.DefaultMessageTimeToLive), expected.DefaultMessageTimeToLive, actual.DefaultMessageTimeToLive, differences);
        Difference(entity, nameof(expected.DuplicateDetectionHistoryTimeWindow), expected.DuplicateDetectionHistoryTimeWindow, actual.DuplicateDetectionHistoryTimeWindow, differences);
        Difference(entity, nameof(expected.EnableBatchedOperations), expected.EnableBatchedOperations, actual.EnableBatchedOperations, differences);
        Difference(entity, nameof(expected.EnablePartitioning), expected.EnablePartitioning, actual.EnablePartitioning, differences);
        Difference(entity, nameof(expected.ForwardDeadLetteredMessagesTo), expected.ForwardDeadLetteredMessagesTo, actual.ForwardDeadLetteredMessagesTo, differences);
        Difference(entity, nameof(expected.ForwardTo), expected.ForwardTo, actual.ForwardTo, differences);
        Difference(entity, nameof(expected.LockDuration), expected.LockDuration, actual.LockDuration, differences);
        Difference(entity, nameof(expected.MaxDeliveryCount), expected.MaxDeliveryCount, actual.MaxDeliveryCount, differences);
        if (expected.MaxMessageSizeInKilobytes is not null) Difference(entity, nameof(expected.MaxMessageSizeInKilobytes), expected.MaxMessageSizeInKilobytes, actual.MaxMessageSizeInKilobytes, differences);
        Difference(entity, nameof(expected.MaxSizeInMegabytes), expected.MaxSizeInMegabytes, actual.MaxSizeInMegabytes, differences);
        Difference(entity, nameof(expected.RequiresDuplicateDetection), expected.RequiresDuplicateDetection, actual.RequiresDuplicateDetection, differences);
        Difference(entity, nameof(expected.RequiresSession), expected.RequiresSession, actual.RequiresSession, differences);
        Difference(entity, nameof(expected.Status), expected.Status, actual.Status, differences);
        Difference(entity, nameof(expected.UserMetadata), expected.UserMetadata, actual.UserMetadata, differences);
        if (!expected.AuthorizationRules.Equals(actual.AuthorizationRules))
            differences.Add($"Entity '{entity}': AuthorizationRules differ. Reconcile rules through infrastructure configuration; keys are never included in the diff.");
    }

    private static void Compare(CreateTopicOptions expected, CreateTopicOptions actual, List<string> differences)
    {
        var entity = expected.Name;
        Difference(entity, nameof(expected.AutoDeleteOnIdle), expected.AutoDeleteOnIdle, actual.AutoDeleteOnIdle, differences);
        Difference(entity, nameof(expected.DefaultMessageTimeToLive), expected.DefaultMessageTimeToLive, actual.DefaultMessageTimeToLive, differences);
        Difference(entity, nameof(expected.DuplicateDetectionHistoryTimeWindow), expected.DuplicateDetectionHistoryTimeWindow, actual.DuplicateDetectionHistoryTimeWindow, differences);
        Difference(entity, nameof(expected.EnableBatchedOperations), expected.EnableBatchedOperations, actual.EnableBatchedOperations, differences);
        Difference(entity, nameof(expected.EnablePartitioning), expected.EnablePartitioning, actual.EnablePartitioning, differences);
        if (expected.MaxMessageSizeInKilobytes is not null) Difference(entity, nameof(expected.MaxMessageSizeInKilobytes), expected.MaxMessageSizeInKilobytes, actual.MaxMessageSizeInKilobytes, differences);
        Difference(entity, nameof(expected.MaxSizeInMegabytes), expected.MaxSizeInMegabytes, actual.MaxSizeInMegabytes, differences);
        Difference(entity, nameof(expected.RequiresDuplicateDetection), expected.RequiresDuplicateDetection, actual.RequiresDuplicateDetection, differences);
        Difference(entity, nameof(expected.Status), expected.Status, actual.Status, differences);
        Difference(entity, nameof(expected.SupportOrdering), expected.SupportOrdering, actual.SupportOrdering, differences);
        Difference(entity, nameof(expected.UserMetadata), expected.UserMetadata, actual.UserMetadata, differences);
        if (!expected.AuthorizationRules.Equals(actual.AuthorizationRules))
            differences.Add($"Entity '{entity}': AuthorizationRules differ. Reconcile rules through infrastructure configuration; keys are never included in the diff.");
    }

    private static void Compare(CreateSubscriptionOptions expected, CreateSubscriptionOptions actual, List<string> differences)
    {
        var entity = expected.TopicName + "/Subscriptions/" + expected.SubscriptionName;
        Difference(entity, nameof(expected.AutoDeleteOnIdle), expected.AutoDeleteOnIdle, actual.AutoDeleteOnIdle, differences);
        Difference(entity, nameof(expected.DeadLetteringOnMessageExpiration), expected.DeadLetteringOnMessageExpiration, actual.DeadLetteringOnMessageExpiration, differences);
        Difference(entity, nameof(expected.DefaultMessageTimeToLive), expected.DefaultMessageTimeToLive, actual.DefaultMessageTimeToLive, differences);
        Difference(entity, nameof(expected.EnableBatchedOperations), expected.EnableBatchedOperations, actual.EnableBatchedOperations, differences);
        Difference(entity, nameof(expected.EnableDeadLetteringOnFilterEvaluationExceptions), expected.EnableDeadLetteringOnFilterEvaluationExceptions, actual.EnableDeadLetteringOnFilterEvaluationExceptions, differences);
        Difference(entity, nameof(expected.ForwardDeadLetteredMessagesTo), expected.ForwardDeadLetteredMessagesTo, actual.ForwardDeadLetteredMessagesTo, differences);
        Difference(entity, nameof(expected.ForwardTo), expected.ForwardTo, actual.ForwardTo, differences);
        Difference(entity, nameof(expected.LockDuration), expected.LockDuration, actual.LockDuration, differences);
        Difference(entity, nameof(expected.MaxDeliveryCount), expected.MaxDeliveryCount, actual.MaxDeliveryCount, differences);
        Difference(entity, nameof(expected.RequiresSession), expected.RequiresSession, actual.RequiresSession, differences);
        Difference(entity, nameof(expected.Status), expected.Status, actual.Status, differences);
        Difference(entity, nameof(expected.UserMetadata), expected.UserMetadata, actual.UserMetadata, differences);
    }
}

