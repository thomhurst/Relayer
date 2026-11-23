using System.Collections.Concurrent;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Tests;
using Relayer.Transports;

namespace Relayer.GooglePubSub.Tests;

public class GooglePubSubSendDurationTests
{
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Records_one_duration_per_native_publish_regardless_of_caller_grouping(bool batch)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var topic = TopicName.FromProjectTopic("relayer-test", TestContext.Current!.Isolation.GetIsolatedName("send-duration").ToLowerInvariant());
        await broker.Publisher.CreateTopicAsync(topic, token);
        var publishes = new PublishObserver();
        await using var provider = CreateProvider(broker, topic, publishes);
        using var capture = new SendDurationCapture(provider.GetRequiredService<RelayerInstrumentation>());
        var transport = provider.GetRequiredService<ITransport>();
        var message = new OutgoingMessage(topic.ToString(), new byte[] { 1 });
        var messages = new[] { message, message, message, message };

        if (batch)
        {
            await transport.SendBatchAsync(messages, token).AsTask().WaitAsync(token);
        }
        else
        {
            await Task.WhenAll(messages.Select(item => transport.SendAsync(item, token).AsTask())).WaitAsync(token);
        }

        await Assert.That(publishes.MessageCounts.ToArray()).IsEquivalentTo(new[] { 2, 2 });
        await Assert.That(capture.Values.Count).IsEqualTo(publishes.MessageCounts.Count);
        await Assert.That(capture.Values.All(value => value.Seconds > 0 &&
            Equals(value.Tags["messaging.system"], "gcp_pubsub") &&
            Equals(value.Tags["messaging.destination.name"], topic.ToString()) &&
            Equals(value.Tags["messaging.operation.name"], "send") &&
            !value.Tags.ContainsKey("error.type"))).IsTrue();
    }

    [Test]
    public async Task Records_native_failure_and_skips_rejected_preparation()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var missingTopic = TopicName.FromProjectTopic("relayer-test", TestContext.Current!.Isolation.GetIsolatedName("failed-send").ToLowerInvariant());
        var publishes = new PublishObserver();
        await using var provider = CreateProvider(broker, missingTopic, publishes);
        using var capture = new SendDurationCapture(provider.GetRequiredService<RelayerInstrumentation>());
        var transport = provider.GetRequiredService<ITransport>();
        var rejected = new OutgoingMessage(missingTopic.ToString(), new byte[] { 1 },
            options: new SendOptions().GooglePubSub(static _ => throw new InvalidOperationException("Preparation failed")));

        await Assert.That(async () => await transport.SendBatchAsync(new[] { rejected }, token)).Throws<InvalidOperationException>();
        await Assert.That(publishes.MessageCounts.Count).IsEqualTo(0);
        await Assert.That(capture.Values.Count).IsEqualTo(0);

        var message = new OutgoingMessage(missingTopic.ToString(), new byte[] { 1 });
        await Assert.That(async () => await transport.SendBatchAsync(new[] { message, message }, token).AsTask().WaitAsync(token)).Throws<RpcException>();

        await Assert.That(publishes.MessageCounts.ToArray()).IsEquivalentTo(new[] { 2 });
        var duration = capture.Values.Single();
        await Assert.That(duration.Seconds > 0).IsTrue();
        await Assert.That(duration.Tags["messaging.destination.name"]).IsEqualTo(missingTopic.ToString());
        await Assert.That(duration.Tags["error.type"]).IsEqualTo("_OTHER");
    }

    private static ServiceProvider CreateProvider(PubSubBroker broker, TopicName topic, PublishObserver observer)
    {
        var publisher = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(broker.Invoker.Intercept(observer)), null, null);
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.GooglePubSub("duration", transport =>
        {
            transport.TopologyMode = TopologyMode.None;
            transport.Options.Configure(options =>
            {
                options.Publisher = publisher;
                options.Subscriber = broker.Subscriber;
                options.PublisherSettings.BatchingSettings = new(2, null, null);
                options.Topics.Add(topic);
            });
        }));
        return services.BuildServiceProvider();
    }

    private sealed class PublishObserver : Interceptor
    {
        internal ConcurrentQueue<int> MessageCounts
        {
            get;
        } = new();

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context, AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            if (request is PublishRequest publish)
            {
                MessageCounts.Enqueue(publish.Messages.Count);
            }

            return continuation(request, context);
        }
    }
}
