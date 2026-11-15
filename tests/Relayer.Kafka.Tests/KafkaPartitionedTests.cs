using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Protocol.Messages;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

// Broker-backed coverage of the adapter over Dekaf's partitioned runtime (D22).
[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaPartitionedTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    public async Task Different_keys_run_concurrently_and_equal_keys_stay_ordered()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topic = await TopicAsync("keys", 1, token);
        // Key A: offsets 0, 2; key B: offset 1.
        await ProduceAsync(topic.Name, token, ([0xA], 0), ([0xB], 1), ([0xA], 2));
        var blockA = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = Channel.CreateUnbounded<long>();
        await using var adapter = Adapter(Native(topic.Name), (delivery, cancellation) => Receive(delivery, cancellation), topic.Name, concurrency: 2);
        await adapter.StartAsync(token);
        try
        {
            // B completes while the first A record is blocked; the second A record must wait for it.
            await Assert.That(await handled.Reader.ReadAsync(token)).IsEqualTo(1L);
            await Assert.That(handled.Reader.TryRead(out _)).IsFalse();
            blockA.SetResult();
            await Assert.That(await handled.Reader.ReadAsync(token)).IsEqualTo(0L);
            await Assert.That(await handled.Reader.ReadAsync(token)).IsEqualTo(2L);
            await adapter.StopAsync(token);
            await Assert.That(await CommittedAsync(topic.Name, 0, token)).IsEqualTo(3L);
        }
        finally { blockA.TrySetResult(); }

        async ValueTask<Disposition> Receive(KafkaDelivery delivery, CancellationToken cancellation)
        {
            if (delivery.Result.Offset == 0) await blockA.Task.WaitAsync(cancellation);
            handled.Writer.TryWrite(delivery.Result.Offset);
            return Disposition.Complete;
        }
    }

    [Test]
    public async Task Failed_record_is_never_committed_and_restart_replays_it_after_completed_peers()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topic = await TopicAsync("failure", 1, token);
        await ProduceAsync(topic.Name, token, Enumerable.Range(0, 8).Select(i => (new[] { (byte)i }, i)).ToArray());
        var gap = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var peer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (var failed = Adapter(Native(topic.Name), (delivery, _) =>
        {
            if (delivery.Result.Offset == 0) return new(gap.Task);
            peer.TrySetResult();
            return new(Disposition.Complete);
        }, topic.Name, concurrency: 2))
        {
            await failed.StartAsync(token);
            await peer.Task.WaitAsync(token);
            gap.SetException(new ApplicationException("simulated durable failure"));
            await Assert.That(async () => await failed.Completion.WaitAsync(token)).Throws<ApplicationException>();
        }
        await Assert.That(await CommittedAsync(topic.Name, 0, token) is null or 0).IsTrue();
        var replayed = Channel.CreateUnbounded<long>();
        await using var restarted = Adapter(Native(topic.Name), (delivery, _) =>
        {
            replayed.Writer.TryWrite(delivery.Result.Offset);
            return new(Disposition.Complete);
        }, topic.Name);
        await restarted.StartAsync(token);
        for (var i = 0; i < 8; i++) await Assert.That(await replayed.Reader.ReadAsync(token)).IsEqualTo((long)i);
        await restarted.StopAsync(token);
        await Assert.That(await CommittedAsync(topic.Name, 0, token)).IsEqualTo(8L);
    }

    [Test]
    public async Task Bounded_stop_cancels_a_stuck_handler_and_restart_replays_from_it()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topic = await TopicAsync("stop", 1, token);
        await ProduceAsync(topic.Name, token, Enumerable.Range(0, 4).Select(i => (new[] { (byte)1 }, i)).ToArray());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<long>();
        var options = new PartitionedProcessingOptions
        {
            Ordering = PartitionedProcessingOrder.Key, CommitPolicy = PartitionCommitPolicy.CommitCompletedPeriodically,
            StopTimeout = TimeSpan.FromMilliseconds(200),
        };
        await using (var adapter = Adapter(Native(topic.Name), async (delivery, cancellation) =>
        {
            if (delivery.Result.Offset == 1)
            {
                entered.TrySetResult();
                await Task.Delay(Timeout.Infinite, cancellation);
            }
            handled.Enqueue(delivery.Result.Offset);
            return Disposition.Complete;
        }, topic.Name, options: options))
        {
            await adapter.StartAsync(token);
            await entered.Task.WaitAsync(token);
            await adapter.StopAsync(token).WaitAsync(TimeSpan.FromSeconds(10), token);
        }
        await Assert.That(handled.ToArray()).IsEquivalentTo(new long[] { 0 });
        // The final commit shares the expired stop budget, so it may not run; it never passes the stuck record.
        var committed = await CommittedAsync(topic.Name, 0, token) ?? 0;
        await Assert.That(committed).IsLessThanOrEqualTo(1L);
        var replayed = Channel.CreateUnbounded<long>();
        await using var restarted = Adapter(Native(topic.Name), (delivery, _) =>
        {
            replayed.Writer.TryWrite(delivery.Result.Offset);
            return new(Disposition.Complete);
        }, topic.Name);
        await restarted.StartAsync(token);
        for (var i = committed; i < 4; i++) await Assert.That(await replayed.Reader.ReadAsync(token)).IsEqualTo(i);
        await restarted.StopAsync(token);
    }

    [Test]
    public async Task Native_commits_preserve_aborted_transaction_gaps_and_leader_epochs()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topic = await TopicAsync("gaps", 1, token);
        await using (var transactional = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
            .WithTransactionalId(topic.Name).BuildAsync(token))
        {
            await transactional.InitTransactionsAsync(token);
            await using (var aborted = transactional.BeginTransaction())
            {
                await aborted.ProduceAsync(new() { Topic = topic.Name, Partition = 0, Value = [0] }, token);
                await aborted.AbortAsync(token);
            }
            await using (var commit = transactional.BeginTransaction())
            {
                await commit.ProduceAsync(new() { Topic = topic.Name, Partition = 0, Value = [1] }, token);
                await commit.CommitAsync(token);
            }
        }
        var offsets = new ConcurrentQueue<long>();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).WithGroupId(topic.Name)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithIsolationLevel(IsolationLevel.ReadCommitted)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).SubscribeTo(topic.Name).Build();
        await using var adapter = Adapter(native, (delivery, _) =>
        {
            offsets.Enqueue(delivery.Result.Offset);
            handled.TrySetResult();
            return new(Disposition.Complete);
        }, topic.Name);
        await adapter.StartAsync(token);
        await handled.Task.WaitAsync(token);
        await adapter.StopAsync(token);
        // Offsets 0/1 are the aborted record and its marker, 2 the committed record, 3 its marker.
        await Assert.That(offsets.ToArray()).IsEquivalentTo(new long[] { 2 });
        await using var reader = Native(topic.Name);
        await reader.InitializeAsync(token);
        var committed = await reader.GetCommittedOffsetsAsync([new(topic.Name, 0)], token);
        await Assert.That(committed[new(topic.Name, 0)].Offset).IsGreaterThanOrEqualTo(3L);
        await Assert.That(committed[new(topic.Name, 0)].LeaderEpoch).IsGreaterThanOrEqualTo(0);
    }

    [Test]
    public async Task Rebalance_under_load_loses_no_record_and_keeps_key_order()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        const int Partitions = 4, Keys = 8, PerKey = 50;
        await using var topic = await TopicAsync("rebalance", Partitions, token);
        var records = new List<(byte[] Key, int Value)>();
        for (var i = 0; i < PerKey; i++)
            for (var key = 0; key < Keys; key++) records.Add(([(byte)key], i));
        await ProduceAsync(topic.Name, null, token, records.ToArray());
        var seen = new ConcurrentDictionary<(int Key, int Value), int>();
        var last = new ConcurrentDictionary<(string Consumer, int Key), int>();
        var violations = 0;
        var halfway = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var first = Adapter(Native(topic.Name), (delivery, _) => Receive("first", delivery), topic.Name, concurrency: 4);
        await using var second = Adapter(Native(topic.Name), (delivery, _) => Receive("second", delivery), topic.Name, concurrency: 4);
        await first.StartAsync(token);
        await halfway.Task.WaitAsync(token);
        await second.StartAsync(token);
        while (seen.Count < records.Count) await Task.Delay(50, token);
        await first.StopAsync(token);
        await second.StopAsync(token);
        await Assert.That(violations).IsEqualTo(0);
        var committed = 0L;
        for (var partition = 0; partition < Partitions; partition++) committed += await CommittedAsync(topic.Name, partition, token) ?? 0;
        await Assert.That(committed).IsEqualTo((long)records.Count);

        async ValueTask<Disposition> Receive(string consumer, KafkaDelivery delivery)
        {
            int key = delivery.Result.Key![0], value = delivery.Result.Value[0];
            // A replay after rebalance may repeat a record, but one consumer never sees a key regress.
            last.AddOrUpdate((consumer, key), value, (_, previous) =>
            {
                if (value < previous) Interlocked.Increment(ref violations);
                return value;
            });
            if (seen.AddOrUpdate((key, value), 1, static (_, count) => count + 1) == 1 && seen.Count >= records.Count / 2)
                halfway.TrySetResult();
            await Task.Yield();
            return Disposition.Complete;
        }
    }

    [Test]
    public async Task Each_runtime_commit_has_one_ambient_settle_span_with_its_record_count()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topic = await TopicAsync("commits", 1, token);
        await ProduceAsync(topic.Name, token, ([1], 0), ([2], 1), ([3], 2));
        var spans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Relayer",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var handled = 0;
        var parents = new ConcurrentQueue<Activity?>();
        await using var adapter = new KafkaConsumer(Native(topic.Name), (_, _) =>
        {
            parents.Enqueue(Activity.Current);
            Interlocked.Increment(ref handled);
            return new(Disposition.Complete);
        }, tracing, new() { System = "kafka", Destination = topic.Name, ConsumerGroup = topic.Name }, KafkaProcessing.Defaults(1), 1);
        await adapter.StartAsync(token);
        while (Volatile.Read(ref handled) < 3) await Task.Delay(20, token);
        await adapter.StopAsync(token);
        var commits = spans.Where(span => span.DisplayName == "commit " + topic.Name).ToArray();
        await Assert.That(commits.Length).IsGreaterThan(0);
        await Assert.That(commits.Sum(span => span.GetTagItem("messaging.batch.message_count") as int? ?? 0)).IsEqualTo(3);
        await Assert.That(commits.All(span => span.Kind == ActivityKind.Client && span.ParentId is null)).IsTrue();
        await Assert.That(parents.All(parent => parent is null || parent.DisplayName != "commit " + topic.Name)).IsTrue();
    }

    private KafkaConsumer Adapter(IKafkaConsumer<byte[], byte[]> native,
        Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive, string topic, int concurrency = 1,
        PartitionedProcessingOptions? options = null)
    {
        var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        return new(native, receive, tracing, new() { System = "kafka", Destination = topic, ConsumerGroup = topic },
            options ?? KafkaProcessing.Defaults(concurrency), concurrency);
    }

    private IKafkaConsumer<byte[], byte[]> Native(string topic) => new ConsumerBuilder<byte[], byte[]>()
        .WithBootstrapServers(broker.Bootstrap).WithGroupId(topic).WithAutoOffsetReset(AutoOffsetReset.Earliest)
        .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).SubscribeTo(topic).Build();

    private async Task<long?> CommittedAsync(string topic, int partition, CancellationToken token)
    {
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).WithGroupId(topic)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await reader.InitializeAsync(token);
        var offset = await reader.GetCommittedOffsetAsync(new(topic, partition), token);
        return offset is null or < 0 ? null : offset;
    }

    // Partition 0 unless keyed partitioning is requested with null.
    private Task ProduceAsync(string topic, CancellationToken token, params (byte[] Key, int Value)[] records)
        => ProduceAsync(topic, 0, token, records);

    private async Task ProduceAsync(string topic, int? partition, CancellationToken token, params (byte[] Key, int Value)[] records)
    {
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).BuildAsync(token);
        foreach (var (key, value) in records)
            await producer.ProduceAsync(new() { Topic = topic, Partition = partition, Key = key, Value = [(byte)value] }, token);
    }

    private async Task<IsolatedTopic> TopicAsync(string name, int partitions, CancellationToken token)
    {
        await broker.Started.WaitAsync(token);
        var topic = TestContext.Current!.Isolation.GetIsolatedName(name).ToLowerInvariant();
        var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }], cancellationToken: token);
        return new(topic, admin);
    }

    private sealed class IsolatedTopic(string name, IAdminClient admin) : IAsyncDisposable
    {
        internal string Name { get; } = name;
        public async ValueTask DisposeAsync()
        {
            try { await admin.DeleteTopicsAsync([Name]); }
            finally { await admin.DisposeAsync(); }
        }
    }
}
