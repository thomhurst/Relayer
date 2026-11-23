using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.GooglePubSub.Tests;

public class GooglePubSubTopologyTests
{
    [Test]
    public async Task Provision_and_validate_accept_native_policy_defaults_and_detect_drift()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("policy-defaults");
        var topic = TopicName.FromProjectTopic("relayer-test", name);
        var deadTopic = TopicName.FromProjectTopic("relayer-test", $"{name}-dead");
        var minimumOnly = DeclareSubscription(topic, $"{name}-minimum");
        minimumOnly.RetryPolicy = new()
        {
            MinimumBackoff = Duration.FromTimeSpan(TimeSpan.FromSeconds(15))
        };
        var maximumOnly = DeclareSubscription(topic, $"{name}-maximum");
        maximumOnly.RetryPolicy = new()
        {
            MaximumBackoff = Duration.FromTimeSpan(TimeSpan.FromSeconds(120))
        };
        var deadLetter = DeclareSubscription(topic, $"{name}-dead");
        deadLetter.DeadLetterPolicy = new()
        {
            DeadLetterTopic = deadTopic.ToString()
        };

        await using var provider = CreateProvider(broker, [topic, deadTopic], minimumOnly, maximumOnly, deadLetter);
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();

        var actualMinimum = await broker.Subscriber.GetSubscriptionAsync(minimumOnly.Name, token);
        var actualMaximum = await broker.Subscriber.GetSubscriptionAsync(maximumOnly.Name, token);
        var actualDeadLetter = await broker.Subscriber.GetSubscriptionAsync(deadLetter.Name, token);
        await Assert.That(actualMinimum.RetryPolicy.MaximumBackoff.Seconds).IsEqualTo(600L);
        await Assert.That(actualMaximum.RetryPolicy.MinimumBackoff.Seconds).IsEqualTo(10L);
        await Assert.That(actualDeadLetter.DeadLetterPolicy.MaxDeliveryAttempts).IsEqualTo(5);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        var changedMinimum = minimumOnly.Clone();
        changedMinimum.RetryPolicy.MinimumBackoff = new Duration();
        var changedMaximum = maximumOnly.Clone();
        changedMaximum.RetryPolicy.MaximumBackoff.Seconds = 121;
        var changedDeadLetter = deadLetter.Clone();
        changedDeadLetter.DeadLetterPolicy.MaxDeliveryAttempts = 6;

        await using var driftProvider = CreateProvider(broker, [topic, deadTopic], changedMinimum, changedMaximum, changedDeadLetter);
        var drift = driftProvider.GetRequiredService<ITransport>();

        foreach (var mode in new[] { TopologyMode.Provision, TopologyMode.Validate })
        {
            var report = await drift.ApplyTopologyAsync(mode, token);
            await Assert.That(report.Differences.Count).IsEqualTo(3);
            await Assert.That(report.Differences.Single(difference => difference.Contains(minimumOnly.Name))).Contains("RetryPolicy");
            await Assert.That(report.Differences.Single(difference => difference.Contains(maximumOnly.Name))).Contains("RetryPolicy");
            await Assert.That(report.Differences.Single(difference => difference.Contains(deadLetter.Name))).Contains("DeadLetterPolicy");
        }

        await using var omittedProvider = CreateProvider(broker, [topic, deadTopic], DeclareSubscription(topic, $"{name}-minimum"), DeclareSubscription(topic, $"{name}-dead"));
        var omitted = omittedProvider.GetRequiredService<ITransport>();
        var omittedReport = await omitted.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(omittedReport.Differences.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Declared_default_expiration_matches_omitted_and_explicit_broker_policies()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("expiration");
        var topic = TopicName.FromProjectTopic("relayer-test", name);
        await broker.Publisher.CreateTopicAsync(topic, token);

        var nativeDefault = DeclareSubscription(topic, $"{name}-omitted");
        var explicitDefault = DeclareSubscription(topic, $"{name}-default");
        var shorter = DeclareSubscription(topic, $"{name}-shorter");
        var neverExpires = DeclareSubscription(topic, $"{name}-never");
        explicitDefault.ExpirationPolicy = new()
        {
            Ttl = Duration.FromTimeSpan(TimeSpan.FromDays(31))
        };
        shorter.ExpirationPolicy = new()
        {
            Ttl = Duration.FromTimeSpan(TimeSpan.FromDays(30))
        };
        neverExpires.ExpirationPolicy = new();

        Subscription[] nativeSubscriptions = [nativeDefault, explicitDefault, shorter, neverExpires];
        var expected = new List<Subscription>();

        foreach (var subscription in nativeSubscriptions)
        {
            await broker.Subscriber.CreateSubscriptionAsync(subscription, token);
            var declaration = subscription.Clone();
            declaration.ExpirationPolicy = explicitDefault.ExpirationPolicy.Clone();
            expected.Add(declaration);
        }

        var before = await broker.Subscriber.GetSubscriptionAsync(nativeDefault.Name, token);
        await Assert.That(before.ExpirationPolicy is null).IsTrue();
        await using var provider = CreateProvider(broker, [topic], expected.ToArray());
        var transport = provider.GetRequiredService<ITransport>();

        foreach (var mode in new[] { TopologyMode.Validate, TopologyMode.Provision })
        {
            var report = await transport.ApplyTopologyAsync(mode, token);
            await Assert.That(report.Differences.Count).IsEqualTo(2);
            await Assert.That(report.Differences.Single(difference => difference.Contains(shorter.Name))).Contains("ExpirationPolicy");
            await Assert.That(report.Differences.Single(difference => difference.Contains(neverExpires.Name))).Contains("ExpirationPolicy");
        }

        var after = await broker.Subscriber.GetSubscriptionAsync(nativeDefault.Name, token);
        await Assert.That(after.ExpirationPolicy is null).IsTrue();
    }

    [Test]
    public async Task Empty_push_config_validates_and_provisions_pull_subscriptions()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("pull");
        var topic = TopicName.FromProjectTopic("relayer-test", name);
        await broker.Publisher.CreateTopicAsync(topic, token);

        var existing = DeclareSubscription(topic, $"{name}-existing");
        existing.PushConfig = new();
        await broker.Subscriber.CreateSubscriptionAsync(existing, token);

        await using var validationProvider = CreateProvider(broker, [topic], existing);
        var validator = validationProvider.GetRequiredService<ITransport>();
        await Assert.That((await validator.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();

        var missing = DeclareSubscription(topic, $"{name}-missing");
        missing.PushConfig = new();
        await using var provisioningProvider = CreateProvider(broker, [topic], existing, missing);
        var provisioner = provisioningProvider.GetRequiredService<ITransport>();
        await Assert.That((await provisioner.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences).IsEmpty();
        var created = await broker.Subscriber.GetSubscriptionAsync(missing.Name, token);
        await Assert.That(created.PushConfig?.Equals(new PushConfig()) == true).IsTrue();
        await Assert.That((await provisioner.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences).IsEmpty();
    }

    [Test]
    public async Task Label_drift_distinguishes_missing_empty_and_different_values()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("labels");
        var topic = TopicName.FromProjectTopic("relayer-test", name);
        await broker.Publisher.CreateTopicAsync(topic, token);

        var existing = DeclareSubscription(topic, name);
        existing.Labels.Add("present", "");
        existing.Labels.Add("different", "retail");
        await broker.Subscriber.CreateSubscriptionAsync(existing, token);

        var declaration = existing.Clone();
        declaration.Labels.Add("missing", "");
        declaration.Labels["different"] = "";
        await using var provider = CreateProvider(broker, [topic], declaration);
        var transport = provider.GetRequiredService<ITransport>();

        foreach (var mode in new[] { TopologyMode.Validate, TopologyMode.Provision })
        {
            var report = await transport.ApplyTopologyAsync(mode, token);
            await Assert.That(report.Differences.Count).IsEqualTo(2);
            await Assert.That(report.Differences.Single(difference => difference.Contains("Labels[missing]")))
                .Contains("expected '', label is missing");
            await Assert.That(report.Differences.Single(difference => difference.Contains("Labels[different]")))
                .Contains("expected '', actual 'retail'");
        }
    }

    private static Subscription DeclareSubscription(TopicName topic, string name)
    {
        return new()
        {
            SubscriptionName = SubscriptionName.FromProjectSubscription(topic.ProjectId, name),
            Topic = topic.ToString()
        };
    }

    private static ServiceProvider CreateProvider(PubSubBroker broker, TopicName[] topics, params Subscription[] subscriptions)
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder => builder.GooglePubSub("pubsub", pubsub =>
        {
            pubsub.Options.Configure(options =>
            {
                options.Publisher = broker.Publisher;
                options.Subscriber = broker.Subscriber;

                foreach (var topic in topics)
                {
                    options.Topics.Add(topic);
                }

                foreach (var subscription in subscriptions)
                {
                    options.Subscriptions.Add(subscription);
                }
            });
        }));
        return services.BuildServiceProvider();
    }
}
