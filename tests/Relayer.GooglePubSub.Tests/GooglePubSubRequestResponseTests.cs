using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Relayer.GooglePubSub.Tests;

public class GooglePubSubRequestResponseTests
{
    [Test]
    public async Task Replies_publish_to_the_topic_and_complete_on_its_subscription()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        const string Project = "relayer-test";
        var name = TestContext.Current!.Isolation.GetIsolatedName("request-response");
        var requestTopic = TopicName.FromProjectTopic(Project, name + "-requests");
        var requestSubscription = SubscriptionName.FromProjectSubscription(Project, name + "-request-consumer");
        var replyTopic = TopicName.FromProjectTopic(Project, name + "-replies");
        var replySubscription = SubscriptionName.FromProjectSubscription(Project, name + "-reply-consumer");
        var deadLetter = TopicName.FromProjectTopic(Project, name + "-dead");
        var failures = new FailureOptions { DeadLetterEndpoint = deadLetter.ToString() };
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.GooglePubSub("rpc", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.Publisher = broker.Publisher;
                    options.Subscriber = broker.Subscriber;
                    options.PublisherSettings.EnableMessageOrdering = true;
                    options.Topics.Add(requestTopic);
                    options.Topics.Add(replyTopic);
                    options.Topics.Add(deadLetter);
                    options.Subscriptions.Add(new Subscription
                    {
                        SubscriptionName = requestSubscription,
                        TopicAsTopicName = requestTopic,
                    });
                    options.Subscriptions.Add(new Subscription
                    {
                        SubscriptionName = replySubscription,
                        TopicAsTopicName = replyTopic,
                    });
                });

                transport.Request<PubSubRequest, PubSubResponse>(requestTopic.ToString(), replySubscription.ToString(), request =>
                {
                    request.ReplyDestination = replyTopic.ToString();
                    request.Replies(consumer => consumer.Failures(failures));
                });
                transport.Send<PubSubResponse>(replyTopic.ToString());
                transport.Consume(requestSubscription.ToString(), consumer =>
                {
                    consumer.Failures(failures);
                    consumer.Handler<PubSubRequestHandler>();
                });
            });
        });

        using var host = builder.Build();
        await host.StartAsync(token);

        try
        {
            await using var scope = host.Services.CreateAsyncScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<PubSubRequest, PubSubResponse>>();
            var response = await client.RequestAsync(new(42), TimeSpan.FromSeconds(30), token);
            await Assert.That(response.Value).IsEqualTo(42);
        }
        finally
        {
            await host.StopAsync(token);
        }
    }
}

[Message("tests.pubsub.request")]
public sealed record PubSubRequest(int Value);

[Message("tests.pubsub.response")]
public sealed record PubSubResponse(int Value);

public sealed class PubSubRequestHandler : IHandler<PubSubRequest>
{
    public ValueTask HandleAsync(PubSubRequest request, MessageContext context, CancellationToken cancellationToken)
    {
        return context.ReplyAsync(new PubSubResponse(request.Value), cancellationToken);
    }
}
