using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Protocol.Messages;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaOffsetOwnershipTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    [Arguments(1, false)]
    [Arguments(3, false)]
    [Arguments(1, true)]
    [Arguments(3, true)]
    public async Task Revoked_native_window_cannot_commit_even_after_last_handler(int revokeAfter, bool grouped)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started;
        var topic = TestContext.Current!.Isolation.GetIsolatedName("offsets").ToLowerInvariant();
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        try
        {
            await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).BuildAsync(token);
            for (var i = 0; i < 3; i++)
                await producer.ProduceAsync(new() { Topic = topic, Partition = 0, Value = [1] }, token);
            var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
                .WithGroupId(topic).WithAutoOffsetReset(AutoOffsetReset.Earliest)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
            var partition = new TopicPartition(topic, 0);
            if (grouped) native.Subscribe(topic);
            else native.Assign(partition);
            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
            var handled = 0;
            var revoke = true;
            await using var adapter = new KafkaConsumer(native, (_, _) =>
            {
                if (++handled == revokeAfter && revoke)
                {
                    // Unsubscribe ends real group membership and publishes revocation;
                    // Unassign exercises the native manually assigned path as well.
                    if (grouped) native.Unsubscribe();
                    else native.Unassign();
                }
                return ValueTask.FromResult(Disposition.Complete);
            }, tracing, new() { System = "kafka", Destination = topic, ConsumerGroup = topic }, new());
            await native.InitializeAsync(token);
            await using (var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator(token))
            {
                await Assert.That(await batches.MoveNextAsync()).IsTrue();
                await Assert.That(await adapter.ProcessBatchAsync(batches.Current, token)).IsEqualTo(0);
                await Assert.That(handled).IsEqualTo(revokeAfter);
                await Assert.That(await native.GetCommittedOffsetAsync(partition, token) is null or < 0).IsTrue();
            }
            revoke = false;
            handled = 0;
            if (grouped) native.Subscribe(topic);
            else native.Assign(partition);
            await using (var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator(token))
            {
                var complete = 0;
                while (complete < 3 && await batches.MoveNextAsync())
                    complete += await adapter.ProcessBatchAsync(batches.Current, token);
                await Assert.That(complete).IsEqualTo(3);
                await Assert.That(handled).IsEqualTo(3);
                await Assert.That(await native.GetCommittedOffsetAsync(partition, token)).IsEqualTo(3L);
            }
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }

    [Test]
    public async Task Native_checkpoint_preserves_hidden_transaction_gaps_and_leader_epoch()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started;
        var topic = TestContext.Current!.Isolation.GetIsolatedName("gaps").ToLowerInvariant();
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        try
        {
            await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
                .WithTransactionalId(topic).BuildAsync(token);
            await producer.InitTransactionsAsync(token);
            await using (var transaction = producer.BeginTransaction())
            {
                await transaction.ProduceAsync(new() { Topic = topic, Partition = 0, Value = [0] }, token);
                await transaction.CommitAsync(token);
            }
            await using (var transaction = producer.BeginTransaction())
            {
                await transaction.ProduceAsync(new() { Topic = topic, Partition = 0, Value = [1] }, token);
                await transaction.AbortAsync(token);
            }
            await using (var transaction = producer.BeginTransaction())
            {
                await transaction.ProduceAsync(new() { Topic = topic, Partition = 0, Value = [2] }, token);
                await transaction.CommitAsync(token);
            }
            var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
                .WithGroupId(topic).WithAutoOffsetReset(AutoOffsetReset.Earliest).WithIsolationLevel(IsolationLevel.ReadCommitted)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
            native.Assign(new TopicPartition(topic, 0));
            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
            var offsets = new List<long>();
            await using var adapter = new KafkaConsumer(native, (delivery, _) =>
            {
                offsets.Add(delivery.Result.Offset);
                return ValueTask.FromResult(Disposition.Complete);
            }, tracing, new() { System = "kafka", Destination = topic, ConsumerGroup = topic }, new());
            await native.InitializeAsync(token);
            await using var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator(token);
            while (offsets.Count < 2 && await batches.MoveNextAsync())
            {
                await adapter.ProcessBatchAsync(batches.Current, token);
                await Assert.That(batches.Current.TryGetNextOffset(out var checkpoint)).IsTrue();
                await Assert.That(await native.GetCommittedOffsetAsync(new(topic, 0), token)).IsEqualTo(checkpoint.Offset);
                await Assert.That(checkpoint.LeaderEpoch).IsGreaterThanOrEqualTo(0);
            }
            await Assert.That(offsets.Count).IsEqualTo(2);
            await Assert.That(offsets[1]).IsGreaterThan(offsets[0] + 1);
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }
}
