using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaRecordSendTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Native_partition_timestamp_and_headers_round_trip(bool batch, bool tombstone)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started;
        var topic = TestContext.Current!.Isolation.GetIsolatedName("records").ToLowerInvariant();
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }], cancellationToken: token);
        try
        {
            var producer = await new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
                .WithBootstrapServers(broker.Bootstrap).WithKeySerializer(new KafkaKeySerializer())
                .WithValueSerializer(new KafkaValueSerializer()).BuildAsync(token);
            await using var transport = new KafkaProducerTransport("records", producer, [topic]);
            var timestamp = DateTimeOffset.UtcNow.AddMinutes(-10);
            timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestamp.ToUnixTimeMilliseconds());
            var options = new SendOptions().Kafka(static h => h.Add("native", "yes"))
                .KafkaRecord(record => record with { Partition = 2, Timestamp = timestamp });
            var message = new OutgoingMessage(topic, "value"u8.ToArray(), orderingKey: new("key"), options: options, isTombstone: tombstone);
            if (batch) await transport.SendBatchAsync(new[] { message, message }, token);
            else await transport.SendAsync(message, token);
            await using var consumer = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
                .WithGroupId(topic).WithAutoOffsetReset(AutoOffsetReset.Earliest)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).Build();
            consumer.Assign(new TopicPartition(topic, 2));
            await consumer.InitializeAsync(token);
            var count = 0;
            await foreach (var records in consumer.ConsumeBatchAsync(token))
            {
                foreach (var record in records)
                {
                    await Assert.That(record.Partition).IsEqualTo(2);
                    await Assert.That(record.Timestamp).IsEqualTo(timestamp);
                    await Assert.That(record.Headers!.Single(header => header.Key == "native").GetValueAsString()).IsEqualTo("yes");
                    await Assert.That(record.Value is null).IsEqualTo(tombstone);
                    if (!tombstone) await Assert.That(System.Text.Encoding.UTF8.GetString(record.Value!)).IsEqualTo("value");
                    count++;
                }
                if (count == (batch ? 2 : 1)) break;
            }
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }
}
