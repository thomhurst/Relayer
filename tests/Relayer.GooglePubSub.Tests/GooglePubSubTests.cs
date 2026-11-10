using System.Text.Json.Serialization;
using System.Threading.Channels;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.GooglePubSub.Tests;

[NotInParallel("PubSubEmulator")]
public class GooglePubSubTests
{
    [Test]
    public async Task Native_binding_borrows_body_and_attributes()
    {
        byte[] body = [1, 2, 3];
        var message = new PubsubMessage { Data = UnsafeByteOperations.UnsafeWrap(body) };
        message.Attributes.Add("ce_type", "test;v=1");
        var delivery = new GooglePubSubDelivery(message, null!);
        body[0] = 9;
        await Assert.That(GooglePubSubBinding.GetBody(delivery).FirstSpan[0]).IsEqualTo((byte)9);
        await Assert.That(GooglePubSubBinding.TryGetHeader(delivery, "ce_type", out var value)).IsTrue();
        await Assert.That(value.Text).IsSameReferenceAs(message.Attributes["ce_type"]);
        await Assert.That(GooglePubSubBinding.GetDeliveryCount(delivery)).IsNull();
        message.Attributes["googclient_deliveryattempt"] = "3";
        await Assert.That(GooglePubSubBinding.GetDeliveryCount(delivery)).IsEqualTo(3);
        await Assert.That(GooglePubSubBinding.TryGetHeader(delivery, "missing", out _)).IsFalse();
    }

    [Test]
    public async Task Native_settlement_rejects_metadata_delay_and_dead_letter()
    {
        await Assert.That(GooglePubSubConsumer.Resolve(Disposition.Redeliver(TimeSpan.Zero))).IsEqualTo(SubscriberClient.Reply.Nack);
        await Assert.That(() => GooglePubSubConsumer.Resolve(Disposition.Redeliver(TimeSpan.Zero, new[] { new KeyValuePair<string, WireString>("error", new("value")) }))).Throws<NotSupportedException>();
        await Assert.That(() => GooglePubSubConsumer.Resolve(Disposition.Redeliver(TimeSpan.FromSeconds(1)))).Throws<NotSupportedException>();
        await Assert.That(() => GooglePubSubConsumer.Resolve(Disposition.DeadLettered(new("failure")))).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Registration_rejects_missing_native_clients_before_connecting()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.GooglePubSub("events", _ => { }));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<InvalidOperationException>().WithMessageContaining("prepared native");
    }

    [Test]
    public async Task Native_topology_rejects_unvalidated_entity_settings()
    {
        await Assert.That(() => GooglePubSubTopology.ValidateDeclaration(new() { Name = "projects/test/subscriptions/orders", Topic = "projects/test/topics/orders", PushConfig = new() { PushEndpoint = "https://example.com" } }))
            .Throws<NotSupportedException>().WithMessageContaining("pull subscriptions");
    }

    [Test]
    public async Task Native_nack_redelivers_unchanged_body_and_creation_attributes()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var lease = await PubSubFixture.CreateAsync(tracing, token);
        var fixture = (PubSubFixture)lease;
        var redelivered = new TaskCompletionSource<PubsubMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        fixture.RegisterConsumer((incoming, _) =>
        {
            if (Interlocked.Increment(ref count) == 1) return new(Disposition.Redeliver(TimeSpan.Zero));
            redelivered.TrySetResult(incoming.Message.Clone());
            return new(Disposition.Complete);
        });
        await fixture.Transport.StartAsync(token);
        var parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        await fixture.Transport.SendAsync(new(fixture.Destination, "payload"u8.ToArray(), new[] { new KeyValuePair<string, WireString>("traceparent", new(parent)) }), token);
        var actual = await redelivered.Task.WaitAsync(token);
        await fixture.Transport.StopAsync(token);
        await Assert.That(actual.Data.ToStringUtf8()).IsEqualTo("payload");
        await Assert.That(actual.Attributes["traceparent"]).IsEqualTo(parent);
        await Assert.That(count).IsGreaterThanOrEqualTo(2);
    }

    [Test]
    public async Task Fatal_native_initial_write_recreates_application_without_replacing_subscription()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var lease = await PubSubFixture.CreateAsync(tracing, token);
        var fixture = (PubSubFixture)lease;
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.RegisterConsumer((_, _) => { delivered.TrySetResult(); return new(Disposition.Complete); });
        var failure = new IOException("The request was aborted.",
            new System.Net.Sockets.SocketException((int)System.Net.Sockets.SocketError.ConnectionReset));
        fixture.InjectInitialWriteFailure(failure);
        var original = fixture.Transport;
        await original.StartAsync(token);
        await Assert.That(async () => await fixture.ConsumerCompletion.WaitAsync(token)).Throws<IOException>();
        await Assert.That(fixture.IsExpectedOutageFailure(failure)).IsTrue();
        await Assert.That(fixture.IsExpectedOutageFailure(new IOException("Unrelated failure"))).IsFalse();
        await Assert.That(async () => await original.StopAsync(token)).Throws<AggregateException>();
        await fixture.ReconnectAsync(token);
        await Assert.That(ReferenceEquals(original, fixture.Transport)).IsFalse();
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(new(fixture.Destination, "recovered"u8.ToArray()), token);
        await delivered.Task.WaitAsync(token);
        await fixture.Transport.StopAsync(token);
    }

    [Test]
    public async Task Shutdown_drains_inflight_processing_before_native_acknowledgement()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = await PubSubFixture.CreateAsync(tracing, token);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.RegisterConsumer(async (incoming, cancellation) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(cancellation);
            if (incoming.Message.Data.ToStringUtf8() != "durable") throw new InvalidOperationException("Borrowed body changed.");
            return Disposition.Complete;
        });
        try
        {
            await fixture.Transport.StartAsync(token);
            await fixture.Transport.SendAsync(new(fixture.Destination, "durable"u8.ToArray()), token);
            await entered.Task.WaitAsync(token);
            var stopping = fixture.Transport.StopAsync(token);
            await Assert.That(stopping.IsCompleted).IsFalse();
            release.TrySetResult();
            await stopping;
        }
        finally { release.TrySetResult(); }
    }

    [Test]
    public async Task Hosted_pipeline_sends_generated_json_and_copies_poison_before_ack()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var topic = "projects/relayer-test/topics/orders";
        var subscription = "projects/relayer-test/subscriptions/orders";
        var deadLetter = topic + "-dead";
        var deadSubscription = subscription + "-dead";
        var handled = new PubSubControl();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(handled);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(PubSubJson.Default);
            relayer.GooglePubSub("events", transport =>
            {
                transport.TopologyMode = TopologyMode.Provision;
                transport.Options.Configure(options =>
                {
                    options.Publisher = broker.Publisher;
                    options.Subscriber = broker.Subscriber;
                    options.PublisherSettings = new() { EnableMessageOrdering = true };
                    options.Topics.Add(TopicName.Parse(topic));
                    options.Topics.Add(TopicName.Parse(deadLetter));
                    options.Subscriptions.Add(new() { Name = subscription, Topic = topic, AckDeadlineSeconds = 10 });
                    options.Subscriptions.Add(new() { Name = deadSubscription, Topic = deadLetter, AckDeadlineSeconds = 10 });
                });
                transport.Send<PubSubOrder>(topic);
                transport.Consume(subscription, consumer =>
                {
                    consumer.Options.Failure = new() { DeadLetterEndpoint = deadLetter, RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0 };
                    consumer.Handler<PubSubHandler>();
                });
            });
        });
        using var host = builder.Build();
        await host.StartAsync(token);
        using (var scope = host.Services.CreateScope())
        {
            var sender = scope.ServiceProvider.GetRequiredService<ISender>();
            await sender.SendNowAsync(new PubSubOrder(-1), static options => options.GooglePubSub(static message => message.OrderingKey = "external-key"), cancellationToken: token);
            await sender.SendNowAsync(new PubSubOrder(42), cancellationToken: token);
        }
        await Assert.That(await handled.Values.Reader.ReadAsync(token)).IsEqualTo(42);
        var dead = await broker.Subscriber.PullAsync(deadSubscription, 1, token);
        while (dead.ReceivedMessages.Count == 0) dead = await broker.Subscriber.PullAsync(deadSubscription, 1, token);
        var poison = dead.ReceivedMessages[0].Message;
        await Assert.That(poison.Data.ToStringUtf8()).Contains("-1");
        await Assert.That(poison.OrderingKey).IsEqualTo("external-key");
        await Assert.That(GooglePubSubBinding.TryGetHeader(new(poison, null!), FailureHeaders.History, out var history)).IsTrue();
        await Assert.That(history.ToString()).Contains("poison");
        await Assert.That(poison.Attributes.Values.All(value => System.Text.Encoding.UTF8.GetByteCount(value) <= 1024)).IsTrue();
        await Assert.That(poison.Attributes.ContainsKey("ce-type")).IsTrue();
        await host.StopAsync(token);
        var transport = host.Services.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        // Existing subscriptions are never rewritten to satisfy a changed declaration.
        var native = await broker.Subscriber.GetSubscriptionAsync(subscription, token);
        native.AckDeadlineSeconds = 20;
        await broker.Subscriber.UpdateSubscriptionAsync(new() { Subscription = native, UpdateMask = new() { Paths = { "ack_deadline_seconds" } } }, token);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Single()).Contains("AckDeadlineSeconds");
    }
}

[Message("tests.pubsub.order")]
public sealed record PubSubOrder(int Value);
public sealed class PubSubControl
{
    internal Channel<int> Values { get; } = Channel.CreateUnbounded<int>();
}
public sealed class PubSubHandler(PubSubControl control) : IHandler<PubSubOrder>
{
    public ValueTask HandleAsync(PubSubOrder message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Value < 0) throw new InvalidOperationException("poison");
        if (context.GooglePubSub().Message.Data.IsEmpty) throw new InvalidOperationException("Native message is missing.");
        control.Values.Writer.TryWrite(message.Value);
        return default;
    }
}
[JsonSerializable(typeof(PubSubOrder))]
internal partial class PubSubJson : JsonSerializerContext;
