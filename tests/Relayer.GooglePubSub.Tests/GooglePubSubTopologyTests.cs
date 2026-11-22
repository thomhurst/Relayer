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
        var minimumOnly = Declare("minimum");
        minimumOnly.RetryPolicy = new()
        {
            MinimumBackoff = Duration.FromTimeSpan(TimeSpan.FromSeconds(15))
        };
        var maximumOnly = Declare("maximum");
        maximumOnly.RetryPolicy = new()
        {
            MaximumBackoff = Duration.FromTimeSpan(TimeSpan.FromSeconds(120))
        };
        var deadLetter = Declare("dead");
        deadLetter.DeadLetterPolicy = new()
        {
            DeadLetterTopic = deadTopic.ToString()
        };

        await using var provider = Provider(minimumOnly, maximumOnly, deadLetter);
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

        await using var driftProvider = Provider(changedMinimum, changedMaximum, changedDeadLetter);
        var drift = driftProvider.GetRequiredService<ITransport>();

        foreach (var mode in new[] { TopologyMode.Provision, TopologyMode.Validate })
        {
            var report = await drift.ApplyTopologyAsync(mode, token);
            await Assert.That(report.Differences.Count).IsEqualTo(3);
            await Assert.That(report.Differences.Single(difference => difference.Contains(minimumOnly.Name))).Contains("RetryPolicy");
            await Assert.That(report.Differences.Single(difference => difference.Contains(maximumOnly.Name))).Contains("RetryPolicy");
            await Assert.That(report.Differences.Single(difference => difference.Contains(deadLetter.Name))).Contains("DeadLetterPolicy");
        }

        await using var omittedProvider = Provider(Declare("minimum"), Declare("dead"));
        var omitted = omittedProvider.GetRequiredService<ITransport>();
        var omittedReport = await omitted.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(omittedReport.Differences.Count).IsEqualTo(2);

        Subscription Declare(string suffix)
        {
            return new()
            {
                SubscriptionName = SubscriptionName.FromProjectSubscription("relayer-test", $"{name}-{suffix}"),
                Topic = topic.ToString()
            };
        }

        ServiceProvider Provider(params Subscription[] subscriptions)
        {
            var services = new ServiceCollection();
            services.AddRelayer(builder => builder.GooglePubSub("pubsub", pubsub =>
            {
                pubsub.Options.Configure(options =>
                {
                    options.Publisher = broker.Publisher;
                    options.Subscriber = broker.Subscriber;
                    options.Topics.Add(topic);
                    options.Topics.Add(deadTopic);

                    foreach (var subscription in subscriptions)
                    {
                        options.Subscriptions.Add(subscription);
                    }
                });
            }));
            return services.BuildServiceProvider();
        }
    }
}
