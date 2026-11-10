using System.Buffers;
using System.Text;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Relayer.Kafka.Tests;

public class KafkaDeadLetterTests
{
    [Test]
    public async Task Named_registration_lists_only_configured_topics_and_validates_requests_before_IO()
    {
        await using var provider = Provider("unused:9092", "orders");
        var manager = provider.GetRequiredKeyedService<IDeadLetterManager>("events");
        await Assert.That(manager.List().Single()).IsEqualTo(new DeadLetterSource("orders.dead-letter", "orders"));
        await Assert.That(async () => await manager.PeekAsync("missing")).Throws<ArgumentException>();
        await Assert.That(async () => await manager.RedriveAsync("orders.dead-letter", maxMessages: 101)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await manager.PurgeAsync("orders.dead-letter", 0)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await manager.PeekAsync("orders.dead-letter", cursor: "other:0:0")).Throws<ArgumentException>();
        await Assert.That(async () => await manager.PeekAsync("orders.dead-letter", cursor: "orders.dead-letter:-1:0")).Throws<ArgumentException>();
    }

    [Test]
    public async Task Ambiguous_shared_dead_letter_topic_is_rejected()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.Kafka("events", kafka =>
        {
            kafka.Consume("one", c => { c.Options.Failure = new() { DeadLetterEndpoint = "shared" }; c.Handler<TombstoneValueHandler>(); });
            kafka.Consume("two", c => { c.Options.Failure = new() { DeadLetterEndpoint = "shared" }; c.Handler<TombstoneValueHandler>(); });
        }));
        await using var provider = services.BuildServiceProvider();
        await Assert.That(() => provider.GetRequiredKeyedService<IDeadLetterManager>("events"))
            .Throws<InvalidOperationException>().WithMessageContaining("multiple redrive destinations");
    }

    [Test]
    public async Task Peek_redrive_progress_filter_and_prefix_purge_use_native_records()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var source = fixture.Destination + ".dead-letter";
        await using var provider = Provider(fixture.Bootstrap, fixture.Destination);
        var manager = provider.GetRequiredKeyedService<IDeadLetterManager>("events");
        await using var producer = new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).Build();
        await producer.InitializeAsync(token);
        var headers = new Headers().Add(new Header("duplicate", new byte[] { 1 })).Add(new Header("duplicate", (byte[]?)null))
            .Add("ce_id", "original");
        await producer.ForTopic(source).ProduceAsync(new byte[] { 0, 255 }, null!, headers, token);
        await producer.ForTopic(source).ProduceAsync(null!, [], new Headers().Add("ce_id", "empty").Add("duplicate", "first").Add(new Header("duplicate", (byte[]?)null)), token);
        await producer.ForTopic(source).ProduceAsync("last"u8.ToArray(), "last"u8.ToArray(), new Headers(), token);
        var first = await manager.PeekAsync(source, 1, cancellationToken: token);
        var native = first.Messages.Single().Features.GetRequired<KafkaDeadLetterFeature>().Record;
        await Assert.That(native.Value).IsNull();
        await Assert.That(native.Key!).IsEquivalentTo(new byte[] { 0, 255 });
        await Assert.That(native.Headers[1].IsValueNull).IsTrue();
        var second = await manager.PeekAsync(source, 1, first.NextCursor, token);
        await Assert.That(second.Messages.Single().Features.GetRequired<KafkaDeadLetterFeature>().Record.Value.Length).IsEqualTo(0);
        await Assert.That((await manager.PeekAsync(source, 1, cancellationToken: token)).Messages.Single().MessageId).IsEqualTo(first.Messages[0].MessageId);

        // A filter cannot acknowledge past an unmatched record in the same partition.
        await Assert.That(await manager.RedriveAsync(source, _ => false, 3, token)).IsEqualTo(0);
        await Assert.That(await manager.RedriveAsync(source, maxMessages: 1, cancellationToken: token)).IsEqualTo(1);
        // New manager instances resume the broker's durable redrive group offset.
        await using var resumedProvider = Provider(fixture.Bootstrap, fixture.Destination);
        var resumed = resumedProvider.GetRequiredKeyedService<IDeadLetterManager>("events");
        await Assert.That(await resumed.RedriveAsync(source, maxMessages: 1, cancellationToken: token)).IsEqualTo(1);
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).Build();
        reader.Assign(new TopicPartition(fixture.Destination, 0));
        await reader.InitializeAsync(token);
        var deleted = (await reader.ConsumeOneAsync(TimeSpan.FromSeconds(10), token))!.Value;
        var empty = (await reader.ConsumeOneAsync(TimeSpan.FromSeconds(10), token))!.Value;
        await Assert.That(deleted.Value).IsNull();
        await Assert.That(deleted.Key!).IsEquivalentTo(native.Key!);
        await Assert.That(deleted.Headers[0].Key).IsEqualTo("duplicate");
        await Assert.That(deleted.Headers[1].IsValueNull).IsTrue();
        await Assert.That(empty.Value.Length).IsEqualTo(0);
        await Assert.That(empty.IsKeyNull).IsTrue();
        // Native tracing may add its own header; assert the original application prefix in order.
        var emptyHeaders = empty.Headers.Where(header => header.Key is "ce_id" or "duplicate").ToArray();
        await Assert.That(emptyHeaders.Length).IsEqualTo(3);
        await Assert.That(emptyHeaders[0].Key).IsEqualTo("ce_id");
        await Assert.That(emptyHeaders[0].GetValueAsString()).IsEqualTo("empty");
        await Assert.That(emptyHeaders[1].GetValueAsString()).IsEqualTo("first");
        await Assert.That(emptyHeaders[2].Key).IsEqualTo("duplicate");
        await Assert.That(emptyHeaders[2].IsValueNull).IsTrue();
        await Assert.That(Encoding.UTF8.GetString(deleted.Headers[2].Value.Span)).IsEqualTo("original");
        // Redrive advances a group, not retention: originals still peek until native deletion.
        await Assert.That((await manager.PeekAsync(source, 3, cancellationToken: token)).Messages.Count).IsEqualTo(3);
        await Assert.That(await manager.PurgeAsync(source, 2, token)).IsEqualTo(2);
        var remaining = await manager.PeekAsync(source, 3, cancellationToken: token);
        await Assert.That(remaining.Messages.Count).IsEqualTo(1);
        await Assert.That(remaining.Messages[0].Features.GetRequired<KafkaDeadLetterFeature>().Record.Offset).IsEqualTo(2L);
        await Assert.That(await resumed.RedriveAsync(source, maxMessages: 3, cancellationToken: token)).IsEqualTo(1);
        await Assert.That(await resumed.RedriveAsync(source, maxMessages: 3, cancellationToken: token)).IsEqualTo(0);
    }

    [Test]
    public async Task Filter_failure_and_cancellation_leave_original_available_for_redrive()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var source = fixture.Destination + ".dead-letter";
        await using var producer = new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).Build();
        await producer.InitializeAsync(token);
        await producer.ProduceAsync(source, "key"u8.ToArray(), "body"u8.ToArray(), token);
        await using var provider = Provider(fixture.Bootstrap, fixture.Destination);
        var manager = provider.GetRequiredKeyedService<IDeadLetterManager>("events");
        await Assert.That(async () => await manager.RedriveAsync(source, _ => throw new InvalidOperationException("filter"), cancellationToken: token))
            .Throws<InvalidOperationException>().WithMessageContaining("filter");
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await manager.RedriveAsync(source, cancellationToken: canceled.Token)).Throws<OperationCanceledException>();
        await Assert.That(await manager.RedriveAsync(source, cancellationToken: token)).IsEqualTo(1);
    }

    [Test]
    public async Task Multi_partition_peek_and_purge_skip_empty_prefixes_and_remain_bounded()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var source = fixture.Destination + ".dead-letter";
        await using var admin = new AdminClientBuilder().WithBootstrapServers(fixture.Bootstrap).Build();
        await admin.CreatePartitionsAsync(new Dictionary<string, int> { [source] = 3 }, token);
        await using var producer = new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).Build();
        await producer.InitializeAsync(token);
        for (var partition = 1; partition <= 2; partition++)
            await producer.ProduceAsync(new Dekaf.Producer.ProducerMessage<byte[], byte[]>
            { Topic = source, Partition = partition, Key = [], Value = [(byte)partition] }, token);
        await using var provider = Provider(fixture.Bootstrap, fixture.Destination);
        var manager = provider.GetRequiredKeyedService<IDeadLetterManager>("events");
        string? cursor = null;
        var records = new List<DeadLetterMessage>();
        do
        {
            var page = await manager.PeekAsync(source, 1, cursor, token);
            records.AddRange(page.Messages);
            cursor = page.NextCursor;
        } while (cursor is not null);
        await Assert.That(records.Count).IsEqualTo(2);
        // EOF events must not consume the record limit and starve nonempty partitions.
        await Assert.That(await manager.RedriveAsync(source, maxMessages: 1, cancellationToken: token)).IsEqualTo(1);
        await Assert.That(await manager.RedriveAsync(source, maxMessages: 1, cancellationToken: token)).IsEqualTo(1);
        await Assert.That(records[0].Features.GetRequired<KafkaDeadLetterFeature>().Record.IsKeyNull).IsFalse();
        await Assert.That(records[0].Features.GetRequired<KafkaDeadLetterFeature>().Record.Key!.Length).IsEqualTo(0);
        // The callback's ordinary group must not make purge skip a retained prefix.
        await admin.AlterConsumerGroupOffsetsAsync("ordinary-consumer", [new(source, 1, 1)], token);
        await Assert.That(await manager.PurgeAsync(source, 1, token)).IsEqualTo(1);
        var retainedFirst = await manager.PeekAsync(source, 1, source + ":1:0", token);
        await Assert.That(retainedFirst.Messages.Count).IsEqualTo(0);
        await Assert.That(await manager.PurgeAsync(source, 1, token)).IsEqualTo(1);
        await Assert.That(await manager.PurgeAsync(source, 1, token)).IsEqualTo(0);
    }

    [Test]
    public async Task Native_send_failure_does_not_commit_redrive_progress()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var source = fixture.Destination + ".dead-letter";
        await using var producer = new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).Build();
        await producer.InitializeAsync(token);
        await producer.ProduceAsync(source, null!, "body"u8.ToArray(), token);
        await using var failing = Provider(fixture.Bootstrap, fixture.Destination, failSend: true);
        await Assert.That(async () => await failing.GetRequiredKeyedService<IDeadLetterManager>("events")
            .RedriveAsync(source, cancellationToken: token)).Throws<InvalidOperationException>().WithMessageContaining("send failed");
        await using var provider = Provider(fixture.Bootstrap, fixture.Destination);
        await Assert.That(await provider.GetRequiredKeyedService<IDeadLetterManager>("events")
            .RedriveAsync(source, cancellationToken: token)).IsEqualTo(1);
    }

    private sealed class FailingSerializer : ISerializer<(ReadOnlyMemory<byte> Body, Headers Headers)?>
    {
        public void Serialize<TWriter>((ReadOnlyMemory<byte> Body, Headers Headers)? value, ref TWriter destination,
            SerializationContext context) where TWriter : IBufferWriter<byte>, allows ref struct =>
            throw new InvalidOperationException("send failed");
    }

    private static ServiceProvider Provider(string bootstrap, string destination, bool failSend = false)
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.Kafka("events", kafka =>
        {
            kafka.Options.Configure(o =>
            {
                o.BootstrapServers.Add(bootstrap);
                o.ConfigureConsumer = native => native.WithGroupId("ordinary-consumer");
                if (failSend) o.ConfigureProducer = native => native.WithValueSerializer(new FailingSerializer());
            });
            kafka.Consume(destination, c => { c.Options.Group = "ordinary-consumer"; c.Handler<TombstoneValueHandler>(); });
        }));
        return services.BuildServiceProvider();
    }
}
