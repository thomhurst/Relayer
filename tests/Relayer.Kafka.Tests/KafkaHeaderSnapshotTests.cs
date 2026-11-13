using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaHeaderSnapshotTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_snapshot_is_shared_isolated_and_safe_after_fetch_release(bool tombstone)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started;
        var topic = TestContext.Current!.Isolation.GetIsolatedName("headers").ToLowerInvariant();
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        try
        {
            await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).BuildAsync(token);
            for (var i = 0; i < 2; i++)
                await producer.ProduceAsync(new()
                {
                    Topic = topic, Key = "key"u8.ToArray(), Value = tombstone ? null! : "{}"u8.ToArray(),
                    Headers = new Headers().Add("ce_id", i.ToString()).Add("duplicate", (byte[]?)null)
                        .Add("duplicate", "second").Add("ce_type", "test;v=1"),
                }, token);
            await using var consumer = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
                .WithGroupId(topic).WithAutoOffsetReset(AutoOffsetReset.Earliest)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).Build();
            consumer.Assign(new TopicPartition(topic, 0));
            await consumer.InitializeAsync(token);
            var deliveries = new List<KafkaDelivery>();
            await using (var batches = consumer.ConsumeBatchAsync(token).GetAsyncEnumerator(token))
            {
                while (deliveries.Count < 2 && await batches.MoveNextAsync())
                    foreach (var record in batches.Current)
                    {
                        var delivery = new KafkaDelivery(record, "0");
                        // A real pooled record creates a distinct view on each native access.
                        await Assert.That(ReferenceEquals(delivery.Headers, record.Headers)).IsFalse();
                        var independentlyConstructed = new KafkaDelivery(record, "0");
                        await Assert.That(ReferenceEquals(delivery.Headers, independentlyConstructed.Headers)).IsFalse();
                        await Assert.That(delivery).IsEqualTo(independentlyConstructed);
                        await Assert.That(delivery.GetHashCode()).IsEqualTo(independentlyConstructed.GetHashCode());
                        await Check(delivery, deliveries.Count.ToString(), tombstone);
                        deliveries.Add(delivery);
                    }
            }
            await Assert.That(deliveries.Count).IsEqualTo(2);
            await Assert.That(ReferenceEquals(deliveries[0].Headers, deliveries[1].Headers)).IsFalse();
            // Both native views were materialized before the fetch was released. Copies
            // retain the safe native snapshot, never another delivery's pooled storage.
            await Task.WhenAll(Check(deliveries[0], "0", tombstone), Check(deliveries[1], "1", tombstone));
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }

    private static async Task Check(KafkaDelivery delivery, string id, bool tombstone)
    {
        var copy = delivery with { PartitionId = "copy" };
        await Task.Yield();
        await Assert.That(ReferenceEquals(copy.Headers, delivery.Headers)).IsTrue();
        await Assert.That(KafkaMessageBinding.TryGetHeader(copy, "ce_id", out var value)).IsTrue();
        await Assert.That(value.ToString()).IsEqualTo(id);
        await Assert.That(KafkaMessageBinding.TryGetHeader(copy, "missing", out _)).IsFalse();
        await Assert.That(KafkaMessageBinding.TryGetHeader(copy, "duplicate", out value)).IsTrue();
        await Assert.That(value.IsEmpty).IsTrue();
        await Assert.That(copy.Headers[1].IsValueNull).IsTrue();
        await Assert.That(copy.Headers[2].Key).IsEqualTo("duplicate");
        await Assert.That(System.Text.Encoding.UTF8.GetString(copy.Headers[2].Value.Span)).IsEqualTo("second");
        await Assert.That(KafkaMessageBinding.IsTombstone(copy)).IsEqualTo(tombstone);
    }
}

public sealed class KafkaHeaderSnapshotBroker : IAsyncDisposable
{
    private readonly KafkaContainer _broker = new KafkaBuilder("apache/kafka:4.0.2")
        .WithVendor(KafkaVendor.ApacheSoftwareFoundation).AfterStartupCopy().Build();
    public Task Started { get; }
    public string Bootstrap => _broker.GetBootstrapAddress();
    public KafkaHeaderSnapshotBroker() => Started = _broker.StartAsync();
    public ValueTask DisposeAsync() => _broker.DisposeAsync();
}
