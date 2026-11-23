using System.Collections.Concurrent;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;
using TUnit.Assertions.Enums;

namespace Relayer.GooglePubSub.Tests;

public class GooglePubSubSendTests
{
    private const string Topic = "projects/test/topics/orders";
    private const string DeadLetterTopic = "projects/test/topics/orders-dead";
    private const string SourceSubscription = "projects/test/subscriptions/orders";

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
        native.Release.TrySetResult(new()
        {
            MessageIds = { "1" }
        });
        await Assert.That(async () => await operation).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Alternating_topics_fill_native_count_batches_without_a_delay_threshold()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        await using var broker = await PubSubBroker.StartAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("mixed-batch");
        var first = TopicName.FromProjectTopic("relayer-test", name + "-first");
        var second = TopicName.FromProjectTopic("relayer-test", name + "-second");
        await broker.Publisher.CreateTopicAsync(first, token);
        await broker.Publisher.CreateTopicAsync(second, token);
        var publisher = new RecordingPublisher(broker.Publisher);
        await using var provider = Provider(publisher, transport => transport.Options.Configure(options =>
        {
            options.PublisherSettings.BatchingSettings = new(2, null, null);
            options.Topics.Clear();
            options.Topics.Add(first);
            options.Topics.Add(second);
            options.Subscriptions.Clear();
        }));
        var transport = provider.GetRequiredService<ITransport>();
        var firstDestination = first.ToString();
        var secondDestination = second.ToString();
        OutgoingMessage[] messages =
        [
            new(firstDestination, new byte[] { 1 }),
            new(secondDestination, new byte[] { 2 }),
            new(firstDestination, new byte[] { 3 }),
            new(secondDestination, new byte[] { 4 })
        ];

        var sending = transport.SendBatchAsync(messages, token).AsTask();

        try
        {
            await sending.WaitAsync(TimeSpan.FromSeconds(10), token);
        }
        finally
        {
            if (!sending.IsCompleted)
            {
                // Flush pending native buffers before disposing the channel, even when the regression times out.
                await deadline.CancelAsync();
                await provider.DisposeAsync();
                await Assert.That(async () => await sending).Throws<OperationCanceledException>();
            }
        }

        var requests = publisher.Requests.ToDictionary(request => request.Topic);
        await Assert.That(requests.Count).IsEqualTo(2);
        await Assert.That(requests[firstDestination].Messages.Select(message => message.Data.Span[0]).ToArray())
            .IsEquivalentTo([(byte)1, (byte)3], CollectionOrdering.Matching);
        await Assert.That(requests[secondDestination].Messages.Select(message => message.Data.Span[0]).ToArray())
            .IsEquivalentTo([(byte)2, (byte)4], CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(Topic)]
    [Arguments(DeadLetterTopic)]
    public async Task Batch_drains_admitted_native_sends_when_a_later_callback_fails(string failingDestination)
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native);
        var transport = provider.GetRequiredService<ITransport>();
        var options = new SendOptions().GooglePubSub(static _ => throw new InvalidOperationException("callback failed"));
        var operation = transport.SendBatchAsync(new OutgoingMessage[] { new(Topic, new byte[] { 1 }), new(failingDestination, new byte[] { 2 }, options: options) }, default).AsTask();
        var request = await native.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(operation.IsCompleted).IsFalse();
        await Assert.That(request.Messages[0].Data.Memory.Span[0]).IsEqualTo((byte)1);
        native.Release.TrySetResult(new()
        {
            MessageIds = { "1" }
        });
        await Assert.That(async () => await operation).Throws<InvalidOperationException>().WithMessageContaining("callback failed");
    }

    [Test]
    [Arguments("")]
    [Arguments("billing")]
    public async Task Invalid_subscription_declaration_identifies_the_name_and_required_format(string name)
    {
        await using var provider = Provider(new GatedPublisher(), transport => transport.Options.Configure(options =>
        {
            options.Subscriptions.Add(new()
            {
                Name = name,
                Topic = Topic
            });
        }));

        var error = await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<ArgumentException>();
        await Assert.That(error!.Message).Contains("Pub/Sub transport 'test'");
        await Assert.That(error.Message).Contains($"Subscriptions[1].Name is '{name}'");
        await Assert.That(error.Message).Contains("projects/{project}/subscriptions/{subscription}");
    }

    [Test]
    [Arguments(false, true, true, false)]
    [Arguments(true, false, false, false)]
    [Arguments(true, true, false, false)]
    [Arguments(true, true, true, true)]
    public async Task Topic_ordering_requires_publisher_and_all_declared_subscriptions(
        bool publisherOrdering, bool firstSubscriptionOrdering, bool secondSubscriptionOrdering, bool expectedTopicOrdering)
    {
        await using var provider = Provider(new GatedPublisher(), transport => transport.Options.Configure(options =>
        {
            options.PublisherSettings.EnableMessageOrdering = publisherOrdering;
            options.Subscriptions[0].EnableMessageOrdering = firstSubscriptionOrdering;
            options.Subscriptions.Add(new()
            {
                Name = SourceSubscription + "-second",
                Topic = Topic,
                EnableMessageOrdering = secondSubscriptionOrdering
            });
        }));
        var transport = provider.GetRequiredService<ITransport>();

        await Assert.That(transport.GetCapabilities(Topic).OrderedByKey).IsEqualTo(expectedTopicOrdering);
        await Assert.That(transport.GetCapabilities(DeadLetterTopic).OrderedByKey).IsEqualTo(publisherOrdering);
        await Assert.That(transport.GetCapabilities(SourceSubscription).OrderedByKey).IsEqualTo(firstSubscriptionOrdering);
        await Assert.That(transport.GetCapabilities(SourceSubscription + "-second").OrderedByKey).IsEqualTo(secondSubscriptionOrdering);
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
        await using var provider = Provider(native, transport => transport.Consume(SourceSubscription, consumer =>
        {
            if (invalid == "concurrency")
            {
                consumer.Concurrency(2);
            }

            if (invalid == "retry")
            {
                consumer.Failures(new()
                {
                    DeadLetterEndpoint = DeadLetterTopic,
                    RedeliveryDelays = new[] { TimeSpan.Zero }
                });
            }
            else if (invalid != "dead-letter")
            {
                consumer.Failures(new()
                {
                    DeadLetterEndpoint = DeadLetterTopic
                });
            }

            if (invalid == "batch")
            {
                consumer.Batch(2, TimeSpan.FromSeconds(1));
            }
        }));

        if (invalid is "retry" or "batch")
        {
            await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<NotSupportedException>();
        }
        else
        {
            await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<InvalidOperationException>();
        }

        await Assert.That(native.Started.Task.IsCompleted).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Consumer_requires_ordering_for_key_preserving_dead_letter_copies(bool enabled)
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native, transport =>
        {
            transport.Options.Configure(options => options.PublisherSettings.EnableMessageOrdering = enabled);
            transport.Consume(SourceSubscription, consumer =>
            {
                consumer.Failures(new()
                {
                    DeadLetterEndpoint = DeadLetterTopic
                });
                consumer.Handler<PubSubHandler>();
            });
        });

        if (enabled)
        {
            await Assert.That(provider.GetRequiredService<ITransport>()).IsNotNull();
        }
        else
        {
            await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<InvalidOperationException>()
                .WithMessageContaining("PublisherSettings.EnableMessageOrdering = true");
        }

        await Assert.That(native.Started.Task.IsCompleted).IsFalse();
    }

    [Test]
    [Arguments("")]
    [Arguments("attributes.kind = \"orders\"")]
    public async Task Same_topic_dead_lettering_requires_a_subscription_filter(string filter)
    {
        var native = new GatedPublisher();
        await using var provider = Provider(native, transport =>
        {
            transport.Options.Configure(options =>
            {
                options.PublisherSettings.EnableMessageOrdering = true;
                options.Subscriptions[0].Filter = filter;
            });
            transport.Consume(SourceSubscription, consumer =>
            {
                consumer.Failures(new()
                {
                    DeadLetterEndpoint = Topic,
                    RetryDelays = []
                });
                consumer.Handler<PubSubHandler>();
            });
        });

        if (filter.Length == 0)
        {
            await Assert.That(() => provider.GetRequiredService<ITransport>()).Throws<InvalidOperationException>()
                .WithMessageContaining("separate dead-letter topic");
        }
        else
        {
            await Assert.That(provider.GetRequiredService<ITransport>()).IsNotNull();
        }
    }

    [Test]
    public async Task Failure_copy_omits_reserved_service_attributes_without_mutating_delivery()
    {
        var message = new PubsubMessage { OrderingKey = "key", Data = Google.Protobuf.ByteString.CopyFromUtf8("body") };
        message.Attributes.Add("googclient_deliveryattempt", "2");
        message.Attributes.Add("goog", "reserved");
        message.Attributes.Add("ce-type", "orders");
        message.Attributes.Add("application", "value");
        var copy = GooglePubSubHostedService.Copy(message);
        await Assert.That(copy.Headers.Length).IsEqualTo(2);
        await Assert.That(copy.Headers.ToArray().All(header => !header.Key.StartsWith("goog", StringComparison.Ordinal))).IsTrue();
        await Assert.That(copy.OrderingKey!.Value.ToString()).IsEqualTo("key");
        await Assert.That(copy.Body.Span.SequenceEqual(message.Data.Span)).IsTrue();
        await Assert.That(message.Attributes.Count).IsEqualTo(4);
    }

    [Test]
    public async Task Pool_retains_concurrency_beyond_one_page()
    {
        var pool = new GooglePubSubPool<object>(static () => new());
        var first = Enumerable.Range(0, 130).Select(_ => pool.Rent()).ToHashSet();

        foreach (var item in first)
        {
            pool.Return(item);
        }

        var second = Enumerable.Range(0, 130).Select(_ => pool.Rent()).ToHashSet();
        await Assert.That(second.SetEquals(first)).IsTrue();
    }

    [Test]
    public async Task Pool_discards_overflow_and_allocates_on_miss()
    {
        var created = 0;
        var pool = new GooglePubSubPool<object>(() =>
        {
            created++;
            return new();
        });
        var burst = Enumerable.Range(0, GooglePubSubPool<object>.Capacity + 1).Select(_ => pool.Rent()).ToArray();

        foreach (var item in burst)
        {
            pool.Return(item);
        }

        var retained = new HashSet<object>();

        for (var i = 0; i < GooglePubSubPool<object>.Capacity; i++)
        {
            retained.Add(pool.Rent());
        }

        await Assert.That(retained.Count).IsEqualTo(GooglePubSubPool<object>.Capacity);
        await Assert.That(retained.SetEquals(burst.Take(GooglePubSubPool<object>.Capacity))).IsTrue();
        var beforeMiss = created;
        var replacement = pool.Rent();
        await Assert.That(created).IsEqualTo(beforeMiss + 1);
        await Assert.That(burst.Contains(replacement)).IsFalse();
        pool.Return(replacement);
        await Assert.That(ReferenceEquals(pool.Rent(), replacement)).IsTrue();
    }

    private static ServiceProvider Provider(PublisherServiceApiClient publisher,
        Action<TransportBuilder<GooglePubSubTransportOptions, GooglePubSubConsumerOptions>>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.GooglePubSub("test", transport =>
        {
            transport.TopologyMode = TopologyMode.None;
            transport.Options.Configure(options =>
            {
                options.Publisher = publisher;
                options.Subscriber = new UnusedSubscriber();
                options.PublisherSettings = new()
                {
                    BatchingSettings = new(1, null, null)
                };
                options.Topics.Add(TopicName.Parse(Topic));
                options.Topics.Add(TopicName.Parse(DeadLetterTopic));
                options.Subscriptions.Add(new()
                {
                    Name = SourceSubscription,
                    Topic = Topic
                });
            });
            configure?.Invoke(transport);
        }));
        return services.BuildServiceProvider();
    }

    private sealed class GatedPublisher : PublisherServiceApiClient
    {
        internal TaskCompletionSource<PublishRequest> Started
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource<PublishResponse> Release
        {
            get;
        } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        {
            Started.TrySetResult(request);
            return Release.Task;
        }
    }

    private sealed class RecordingPublisher(PublisherServiceApiClient inner) : PublisherServiceApiClient
    {
        internal ConcurrentQueue<PublishRequest> Requests
        {
            get;
        } = new();

        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        {
            Requests.Enqueue(request.Clone());
            return inner.PublishAsync(request, callSettings);
        }
    }

    private sealed class UnusedSubscriber : SubscriberServiceApiClient;
}
