using Google.Cloud.PubSub.V1;
using Grpc.Core;

namespace Relayer.GooglePubSub;

internal static class GooglePubSubTopology
{
    internal static void ValidateDeclaration(Subscription subscription)
    {
        // Fail closed for native entity kinds/options this topology validator does not inspect.
        // Provision passes the caller's message unchanged; no alternate subscription options model exists.
        var supported = new Subscription
        {
            Name = subscription.Name, Topic = subscription.Topic, AckDeadlineSeconds = subscription.AckDeadlineSeconds,
            EnableMessageOrdering = subscription.EnableMessageOrdering, EnableExactlyOnceDelivery = subscription.EnableExactlyOnceDelivery,
            Filter = subscription.Filter, DeadLetterPolicy = subscription.DeadLetterPolicy, RetryPolicy = subscription.RetryPolicy,
            RetainAckedMessages = subscription.RetainAckedMessages, MessageRetentionDuration = subscription.MessageRetentionDuration,
            ExpirationPolicy = subscription.ExpirationPolicy,
        };
        supported.Labels.Add(subscription.Labels);
        if (!supported.Equals(subscription))
            throw new NotSupportedException($"Pub/Sub subscription '{subscription.Name}': only pull subscriptions with acknowledgement, ordering, exactly-once, filter, dead-letter, retry, retention, expiration and label settings are validated. Remove other fields from this declaration and manage them externally.");
    }

    internal static async ValueTask<TopologyReport> ApplyAsync(GooglePubSubTransportOptions options,
        IEnumerable<Subscription> subscriptions, TopologyMode mode, CancellationToken token)
    {
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        token.ThrowIfCancellationRequested();
        if (mode == TopologyMode.None) return new([]);
        var differences = new List<string>();
        foreach (var topic in options.Topics)
        {
            try { await options.Publisher!.GetTopicAsync(topic, token).ConfigureAwait(false); }
            catch (RpcException error) when (error.StatusCode == StatusCode.NotFound)
            {
                if (mode == TopologyMode.Validate) differences.Add($"Missing topic '{topic}'. Create it externally or use Provision.");
                else
                {
                    try { await options.Publisher!.CreateTopicAsync(topic, token).ConfigureAwait(false); }
                    catch (RpcException race) when (race.StatusCode == StatusCode.AlreadyExists) { }
                }
            }
        }
        foreach (var expected in subscriptions)
        {
            Subscription actual;
            try { actual = await options.Subscriber!.GetSubscriptionAsync(expected.Name, token).ConfigureAwait(false); }
            catch (RpcException error) when (error.StatusCode == StatusCode.NotFound)
            {
                if (mode == TopologyMode.Validate)
                {
                    differences.Add($"Missing subscription '{expected.Name}'. Create it externally or use Provision.");
                    continue;
                }
                try { actual = await options.Subscriber!.CreateSubscriptionAsync(expected, token).ConfigureAwait(false); }
                catch (RpcException race) when (race.StatusCode == StatusCode.AlreadyExists)
                { actual = await options.Subscriber!.GetSubscriptionAsync(expected.Name, token).ConfigureAwait(false); }
            }
            Check(actual.Topic == expected.Topic, "Topic", expected.Topic, actual.Topic);
            Check(actual.EnableMessageOrdering == expected.EnableMessageOrdering, "EnableMessageOrdering", expected.EnableMessageOrdering, actual.EnableMessageOrdering);
            Check(actual.EnableExactlyOnceDelivery == expected.EnableExactlyOnceDelivery, "EnableExactlyOnceDelivery", expected.EnableExactlyOnceDelivery, actual.EnableExactlyOnceDelivery);
            Check(actual.Filter == expected.Filter, "Filter", expected.Filter, actual.Filter);
            Check(actual.RetainAckedMessages == expected.RetainAckedMessages, "RetainAckedMessages", expected.RetainAckedMessages, actual.RetainAckedMessages);
            Check(Equals(actual.DeadLetterPolicy, expected.DeadLetterPolicy), "DeadLetterPolicy", expected.DeadLetterPolicy, actual.DeadLetterPolicy);
            Check(Equals(actual.RetryPolicy, expected.RetryPolicy), "RetryPolicy", expected.RetryPolicy, actual.RetryPolicy);
            if (expected.AckDeadlineSeconds != 0)
                Check(actual.AckDeadlineSeconds == expected.AckDeadlineSeconds, "AckDeadlineSeconds", expected.AckDeadlineSeconds, actual.AckDeadlineSeconds);
            if (expected.MessageRetentionDuration is not null)
                Check(Equals(actual.MessageRetentionDuration, expected.MessageRetentionDuration), "MessageRetentionDuration", expected.MessageRetentionDuration, actual.MessageRetentionDuration);
            if (expected.ExpirationPolicy is not null)
                Check(Equals(actual.ExpirationPolicy, expected.ExpirationPolicy), "ExpirationPolicy", expected.ExpirationPolicy, actual.ExpirationPolicy);
            foreach (var label in expected.Labels)
                Check(actual.Labels.TryGetValue(label.Key, out var value) && value == label.Value, "Labels[" + label.Key + "]", label.Value, value);
            if (actual.PushConfig is { PushEndpoint.Length: > 0 } || actual.BigqueryConfig is not null || actual.CloudStorageConfig is not null)
                differences.Add($"Subscription '{expected.Name}': expected native pull delivery; actual subscription uses push or export delivery.");

            void Check(bool equal, string field, object? wanted, object? found)
            {
                if (!equal) differences.Add($"Subscription '{expected.Name}' {field}: expected '{wanted}', actual '{found}'. Reconcile externally; Provision never mutates existing entities.");
            }
        }
        return new(differences);
    }
}
