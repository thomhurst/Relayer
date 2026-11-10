using System.Text.Json.Serialization;
using Google.Cloud.PubSub.V1;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.GooglePubSub;
using Relayer.Transports;

using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
var token = deadline.Token;
var address = Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST") ?? throw new InvalidOperationException("Set PUBSUB_EMULATOR_HOST to the test emulator.");
using var channel = GrpcChannel.ForAddress("http://" + address);
var publisher = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(channel), null, null);
var subscriber = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(channel), null, null);
var id = "aot-" + Guid.NewGuid().ToString("N");
var topic = TopicName.FromProjectTopic("relayer-test", id).ToString();
var subscription = SubscriptionName.FromProjectSubscription("relayer-test", id).ToString();
var completion = new SmokeCompletion();
var builder = Host.CreateApplicationBuilder();
builder.Services.AddSingleton(completion);
builder.Services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.UseJsonSerializer(SmokeJson.Default);
    relayer.GooglePubSub("events", transport =>
    {
        transport.TopologyMode = TopologyMode.Provision;
        transport.Options.Configure(options =>
        {
            options.Publisher = publisher;
            options.Subscriber = subscriber;
            options.PublisherSettings = new() { EnableMessageOrdering = true };
            options.Topics.Add(TopicName.Parse(topic));
            options.Topics.Add(TopicName.Parse(topic + "-dead"));
            options.Subscriptions.Add(new() { Name = subscription, Topic = topic, AckDeadlineSeconds = 10 });
        });
        transport.Send<SmokeOrder>(topic);
        transport.Consume(subscription, consumer =>
        {
            consumer.Options.Failure = new() { DeadLetterEndpoint = topic + "-dead" };
            consumer.Handler<SmokeHandler>();
        });
    });
});
using var host = builder.Build();
try
{
    await host.StartAsync(token);
    using (var scope = host.Services.CreateScope())
        await scope.ServiceProvider.GetRequiredService<ISender>().SendNowAsync(new SmokeOrder(42), cancellationToken: token);
    if (await completion.Value.Task.WaitAsync(token) != 42) throw new InvalidOperationException("AOT round trip changed the payload.");
    await host.StopAsync(token);
    if ((await host.Services.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count != 0)
        throw new InvalidOperationException("AOT topology validation failed.");
    Console.WriteLine("Google Pub/Sub Native AOT round trip passed.");
}
finally
{
    await subscriber.DeleteSubscriptionAsync(subscription, CancellationToken.None);
    await publisher.DeleteTopicAsync(topic, CancellationToken.None);
    await publisher.DeleteTopicAsync(topic + "-dead", CancellationToken.None);
}

[Message("tests.pubsub.aot")]
public sealed record SmokeOrder(int Value);
public sealed class SmokeCompletion
{
    internal TaskCompletionSource<int> Value { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
public sealed class SmokeHandler(SmokeCompletion completion) : IHandler<SmokeOrder>
{
    public ValueTask HandleAsync(SmokeOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        if (context.GooglePubSub().Message.Attributes["ce-type"] != "tests.pubsub.aot;v=1")
            throw new InvalidOperationException("CloudEvents type changed.");
        completion.Value.TrySetResult(message.Value);
        return default;
    }
}
[JsonSerializable(typeof(SmokeOrder))]
internal partial class SmokeJson : JsonSerializerContext;
