using Relayer.Pipeline;
using Relayer.Transports;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Microsoft.Extensions.Options;

namespace Relayer.Kafka.Tests;

[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaShutdownTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    [Arguments(0, false, false)]
    [Arguments(1, false, false)]
    [Arguments(0, true, false)]
    [Arguments(1, true, false)]
    [Arguments(0, false, true)]
    [Arguments(1, false, true)]
    public async Task Stop_commits_only_the_durable_prefix_and_restart_replays_the_rest(int completed, bool abort, bool fail)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.Started.WaitAsync(token);
        var topic = TestContext.Current!.Isolation.GetIsolatedName("shutdown").ToLowerInvariant();
        var partition = new TopicPartition(topic, 0);
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        try
        {
            await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).BuildAsync(token);
            for (var i = 0; i < 3; i++)
                await producer.ProduceAsync(new() { Topic = topic, Value = new byte[] { (byte)i } }, token);
            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handled = new List<long>();
            var native = CreateConsumer();
            native.Assign(partition);
            await using (var adapter = new KafkaConsumer(native, Receive, tracing,
                new() { System = "kafka", Destination = topic, ConsumerGroup = topic }, new()))
            {
                try
                {
                    await adapter.StartAsync(token);
                    await entered.Task.WaitAsync(token);
                    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
                    var stopping = adapter.StopAsync(deadline.Token);
                    await Assert.That(stopping.IsCompleted).IsFalse();
                    if (abort)
                    {
                        await deadline.CancelAsync();
                        await Assert.ThrowsAsync<OperationCanceledException>(() => stopping);
                    }
                    else release.SetResult();
                    if (fail)
                    {
                        await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.Completion.WaitAsync(token));
                        await Assert.ThrowsAsync<InvalidOperationException>(() => stopping);
                    }
                    else
                    {
                        await adapter.Completion.WaitAsync(token);
                        if (!abort) await stopping;
                    }
                    var checkpoint = await native.GetCommittedOffsetAsync(partition, token);
                    if (abort || fail) await Assert.That(checkpoint is null or < 0).IsTrue();
                    else await Assert.That(checkpoint).IsEqualTo((long)completed + 1);
                    await Assert.That(handled.ToArray()).IsEquivalentTo(Enumerable.Range(0, completed + 1).Select(i => (long)i).ToArray());
                }
                finally { release.TrySetResult(); }
            }
            await using var restarted = CreateConsumer();
            restarted.Assign(partition);
            await restarted.InitializeAsync(token);
            var replay = new List<long>();
            var first = abort || fail ? 0 : completed + 1;
            await using (var batches = restarted.ConsumeBatchAsync(token).GetAsyncEnumerator(token))
                while (replay.Count < 3 - first && await batches.MoveNextAsync())
                    foreach (var record in batches.Current) replay.Add(record.Offset);
            await Assert.That(replay.ToArray()).IsEquivalentTo(Enumerable.Range(first, 3 - first).Select(i => (long)i).ToArray());

            IKafkaConsumer<byte[], byte[]> CreateConsumer() => new ConsumerBuilder<byte[], byte[]>()
                .WithBootstrapServers(broker.Bootstrap).WithGroupId(topic).WithAutoOffsetReset(AutoOffsetReset.Earliest)
                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();

            async ValueTask<Disposition> Receive(KafkaDelivery delivery, CancellationToken cancellationToken)
            {
                handled.Add(delivery.Result.Offset);
                if (delivery.Result.Offset == completed)
                {
                    entered.SetResult();
                    await release.Task.WaitAsync(cancellationToken);
                    if (fail) throw new InvalidOperationException("Durable receive failed.");
                }
                return Disposition.Complete;
            }
        }
        finally { await admin.DeleteTopicsAsync([topic], cancellationToken: token); }
    }
}
