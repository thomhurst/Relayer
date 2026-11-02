using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.GooglePubSub.Tests;

public class GooglePubSubSendTests
{
    private const string Topic = "projects/test/topics/orders";

    [Test]
    public async Task Cancellation_after_admission_keeps_borrowed_storage_until_native_completion()
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native);
        var transport = provider.GetRequiredService<ITransport>();
        using var cancellation = new CancellationTokenSource();
        byte[] body = [1, 2, 3];
        var operation = transport.SendAsync(new(Topic, body), cancellation.Token).AsTask();
        var request = await native.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await cancellation.CancelAsync();
        await Assert.That(operation.IsCompleted).IsFalse();
        await Assert.That(request.Messages[0].Data.Memory.Span.SequenceEqual(body)).IsTrue();
        native.Release.TrySetResult(new() { MessageIds = { "1" } });
        await Assert.That(async () => await operation).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Batch_drains_admitted_native_sends_when_a_later_callback_fails()
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native);
        var transport = provider.GetRequiredService<ITransport>();
        var options = new SendOptions().GooglePubSub(static _ => throw new InvalidOperationException("callback failed"));
        var operation = transport.SendBatchAsync(new OutgoingMessage[] { new(Topic, new byte[] { 1 }), new(Topic, new byte[] { 2 }, options: options) }, default).AsTask();
        var request = await native.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(operation.IsCompleted).IsFalse();
        await Assert.That(request.Messages[0].Data.Memory.Span[0]).IsEqualTo((byte)1);
        native.Release.TrySetResult(new() { MessageIds = { "1" } });
        await Assert.That(async () => await operation).Throws<InvalidOperationException>().WithMessageContaining("callback failed");
    }

    [Test]
    public async Task Unsupported_sends_and_pre_cancelled_sends_never_reach_native_publish()
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native);
        var transport = provider.GetRequiredService<ITransport>();
        await Assert.That(async () => await transport.SendAsync(new(Topic, default, isTombstone: true), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendAsync(new(Topic, default, deliverAt: DateTimeOffset.UtcNow), default)).Throws<NotSupportedException>();
        await Assert.That(async () => await transport.SendAsync(new(Topic, default, orderingKey: new WireString("key")), default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendAsync(new(Topic, default), new(true))).Throws<OperationCanceledException>();
        await Assert.That(native.Started.Task.IsCompleted).IsFalse();
    }

    [Test]
    public async Task Cancellation_from_native_callback_prevents_admission()
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native);
        using var cancellation = new CancellationTokenSource();
        var options = new SendOptions().GooglePubSub(_ => cancellation.Cancel());
        await Assert.That(async () => await provider.GetRequiredService<ITransport>().SendAsync(new(Topic, default, options: options), cancellation.Token))
            .Throws<OperationCanceledException>();
        await Assert.That(native.Started.Task.IsCompleted).IsFalse();
    }

    [Test]
    [Arguments("concurrency")]
    [Arguments("retry")]
    [Arguments("dead-letter")]
    [Arguments("batch")]
    public async Task Unsupported_consumer_guarantees_fail_before_any_native_receive(string invalid)
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native, transport => transport.Consume("projects/test/subscriptions/orders", consumer =>
        {
            if (invalid == "concurrency") consumer.Concurrency(2);
            if (invalid == "retry") consumer.Options.Failure = new() { DeadLetterEndpoint = Topic, RedeliveryDelays = new[] { TimeSpan.Zero } };
            else if (invalid != "dead-letter") consumer.Options.Failure = new() { DeadLetterEndpoint = Topic };
            if (invalid == "batch") consumer.Batch(2, TimeSpan.FromSeconds(1));
        }));
        if (invalid is "retry" or "batch")
            await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<NotSupportedException>();
        else
            await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<InvalidOperationException>();
        await Assert.That(native.Started.Task.IsCompleted).IsFalse();
    }

    [Test]
    public async Task Pool_retains_concurrency_beyond_one_page()
    {
        var pool = new GooglePubSubPool<object>(static () => new());
        var first = Enumerable.Range(0, 130).Select(_ => pool.Rent()).ToHashSet();
        foreach (var item in first) pool.Return(item);
        var second = Enumerable.Range(0, 130).Select(_ => pool.Rent()).ToHashSet();
        await Assert.That(second.SetEquals(first)).IsTrue();
    }

    private static ServiceProvider Provider(PublisherServiceApiClient publisher,
        Action<TransportBuilder<GooglePubSubTransportOptions, GooglePubSubRouteOptions, GooglePubSubConsumerOptions>>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.GooglePubSub("test", transport =>
        {
            transport.TopologyMode = TopologyMode.None;
            transport.Options.Configure(options =>
            {
                options.Publisher = publisher;
                options.Subscriber = new UnusedSubscriber();
                options.PublisherSettings = new() { BatchingSettings = new(1, null, null) };
                options.Topics.Add(TopicName.Parse(Topic));
                options.Subscriptions.Add(new() { Name = "projects/test/subscriptions/orders", Topic = Topic });
            });
            configure?.Invoke(transport);
        }));
        return services.BuildServiceProvider();
    }

    private sealed class GatedPublisher : PublisherServiceApiClient
    {
        internal TaskCompletionSource<PublishRequest> Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource<PublishResponse> Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        {
            Started.TrySetResult(request);
            return Release.Task;
        }
    }
    private sealed class UnusedSubscriber : SubscriberServiceApiClient;
}
