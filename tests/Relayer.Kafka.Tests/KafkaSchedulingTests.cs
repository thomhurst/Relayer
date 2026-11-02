using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.Options;

namespace Relayer.Kafka.Tests;

public class KafkaSchedulingTests
{
    [Test]
    public async Task Pump_signal_does_not_lose_concurrent_completions_or_allocate_per_wait()
    {
        var signal = new KafkaPumpSignal();
        for (var i = 0; i < 1000; i++) { signal.Signal(); await signal.WaitAsync(); }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) { signal.Signal(); signal.WaitAsync().GetAwaiter().GetResult(); }
        await Assert.That(GC.GetAllocatedBytesForCurrentThread() - before).IsEqualTo(0L);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var stop = timeout.Token.Register(signal.Signal);
        var acknowledgments = Channel.CreateUnbounded<int>();
        var published = 0;
        var producer = Task.Run(async () =>
        {
            for (var i = 1; i <= 10000; i++)
            {
                Volatile.Write(ref published, i);
                signal.Signal();
                if (await acknowledgments.Reader.ReadAsync(timeout.Token) != i) throw new InvalidOperationException("Lost signal.");
            }
        });
        for (var i = 1; i <= 10000; i++)
        {
            while (Volatile.Read(ref published) < i)
            {
                timeout.Token.ThrowIfCancellationRequested();
                await signal.WaitAsync();
            }
            acknowledgments.Writer.TryWrite(i);
        }
        await producer.WaitAsync(timeout.Token);
    }

    [Test]
    [Arguments(true, false)]
    [Arguments(true, true)]
    [Arguments(false, false)]
    public async Task Cooperative_rebalance_drains_paused_partitions_without_skipping_keys(bool pauseBeforeRebalance, bool expireDrain)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var topic = fixture.Destination + ".parallel";
        await using var admin = new AdminClientBuilder().WithBootstrapServers(fixture.Bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 2, ReplicationFactor = 1 }], cancellationToken: token);
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).BuildAsync(token);
        const int count = 24;
        for (var offset = 0; offset < count; offset++)
            for (var partition = 0; partition < 2; partition++)
                await producer.ProduceAsync(new()
                {
                    Topic = topic, Partition = partition, Key = Encoding.UTF8.GetBytes("key-" + offset % 2),
                    Value = Encoding.UTF8.GetBytes(offset.ToString()), Headers = new Headers().Add("sequence", offset.ToString()),
                }, token);

        var entered = new ConcurrentDictionary<int, bool>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = Channel.CreateUnbounded<(int Partition, long Offset)>();
        var firstOrder = new ConcurrentDictionary<(int Partition, string Key), long>();
        var probe = new Rebalances();
        var ownership = new KafkaOwnership();
        var firstNative = Native(fixture.Bootstrap, topic, ownership, probe);
        var options = new KafkaConsumerOptions
        {
            ConcurrencyPerPartition = 2, MaxBufferedPerPartition = pauseBeforeRebalance ? 4 : 64,
            MaxBufferedDeliveries = pauseBeforeRebalance ? 8 : 128,
            DrainTimeout = expireDrain ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromSeconds(20),
        };
        await using var first = new KafkaConsumer(firstNative, First, tracing,
            new() { System = "kafka", Destination = topic, ConsumerGroup = topic }, ownership, options: options, concurrency: 4);
        await first.StartAsync(token);
        try
        {
            await WaitUntilAsync(() => entered.Count == 2 && firstNative.Paused.Count == (pauseBeforeRebalance ? 2 : 0), token);
            var secondOwnership = new KafkaOwnership();
            var secondNative = Native(fixture.Bootstrap, topic, secondOwnership);
            await using var second = new KafkaConsumer(secondNative, Second, tracing,
                new() { System = "kafka", Destination = topic, ConsumerGroup = topic }, secondOwnership, options: options, concurrency: 4);
            await second.StartAsync(token);
            var revoked = await probe.Revoked.Reader.ReadAsync(token);
            await Assert.That(revoked.Length).IsEqualTo(1);
            if (expireDrain)
                await WaitUntilAsync(() => secondNative.Assignment.Count == 1, token);
            release.TrySetResult();
            var seen = new HashSet<(int Partition, long Offset)>();
            while (seen.Count < count * 2) seen.Add(await completed.Reader.ReadAsync(token));
            await first.StopAsync(token);
            await second.StopAsync(token);
            var offsets = await firstNative.GetCommittedOffsetsAsync([new(topic, 0), new(topic, 1)], token);
            for (var partition = 0; partition < 2; partition++)
                await Assert.That(offsets[new(topic, partition)].Offset).IsEqualTo((long)count);
            await Assert.That(seen.Count).IsEqualTo(count * 2);
        }
        finally { release.TrySetResult(); }

        async ValueTask<Disposition> First(KafkaDelivery delivery, CancellationToken cancellation)
        {
            var record = delivery.Result;
            if (record.Offset == 0)
            {
                entered.TryAdd(record.Partition, true);
                await release.Task.WaitAsync(cancellation);
            }
            cancellation.ThrowIfCancellationRequested();
            // Read headers after the pump has advanced/released their original fetch windows.
            if (record.Headers[0].GetValueAsString() != record.Offset.ToString()) throw new InvalidOperationException("Expired headers.");
            var key = (record.Partition, Encoding.UTF8.GetString(record.Key!));
            var previous = firstOrder.GetOrAdd(key, -1);
            if (record.Offset <= previous) throw new InvalidOperationException("Same-key order changed.");
            firstOrder[key] = record.Offset;
            completed.Writer.TryWrite((record.Partition, record.Offset));
            return Disposition.Complete;
        }

        ValueTask<Disposition> Second(KafkaDelivery delivery, CancellationToken cancellation)
        {
            completed.Writer.TryWrite((delivery.Result.Partition, delivery.Result.Offset));
            return new(Disposition.Complete);
        }
    }

    [Test]
    public async Task Native_checkpoints_preserve_aborted_offset_gaps_and_leader_epochs()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await using var transactional = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithTransactionalId(fixture.Destination).BuildAsync(token);
        await transactional.InitTransactionsAsync(token);
        await using (var aborted = transactional.BeginTransaction())
        {
            await aborted.ProduceAsync(new() { Topic = fixture.Destination, Partition = 0, Key = [0], Value = [0] }, token);
            await aborted.AbortAsync(token);
        }
        await using (var committed = transactional.BeginTransaction())
        {
            await committed.ProduceAsync(new() { Topic = fixture.Destination, Partition = 0, Key = [1], Value = [1] }, token);
            await committed.CommitAsync(token);
        }
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).BuildAsync(token);
        await producer.ProduceAsync(new() { Topic = fixture.Destination, Partition = 0, Key = [2], Value = [2] }, token);
        var ownership = new KafkaOwnership();
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).WithGroupId(fixture.Destination)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithIsolationLevel(Dekaf.Protocol.Messages.IsolationLevel.ReadCommitted)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false)
            .AddRebalanceListener(ownership).SubscribeTo(fixture.Destination).Build();
        var release = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var later = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var offsets = new ConcurrentQueue<long>();
        await using var adapter = new KafkaConsumer(native, (delivery, _) =>
        {
            offsets.Enqueue(delivery.Result.Offset);
            if (delivery.Result.Offset == 2) return new(release.Task);
            later.TrySetResult();
            return new(Disposition.Complete);
        }, tracing, new() { System = "kafka", Destination = fixture.Destination }, ownership,
            options: new() { ConcurrencyPerPartition = 2 }, concurrency: 2);
        await adapter.StartAsync(token);
        try
        {
            await later.Task.WaitAsync(token);
            var before = await native.GetCommittedOffsetsAsync([new(fixture.Destination, 0)], token);
            await Assert.That(!before.TryGetValue(new(fixture.Destination, 0), out var position) || position.Offset < 2).IsTrue();
            release.SetResult(Disposition.Complete);
            await adapter.StopAsync(token);
            var after = await native.GetCommittedOffsetsAsync([new(fixture.Destination, 0)], token);
            await Assert.That(offsets.ToArray()).IsEquivalentTo(new long[] { 2, 4 });
            await Assert.That(after[new(fixture.Destination, 0)].Offset).IsEqualTo(5L);
            await Assert.That(after[new(fixture.Destination, 0)].LeaderEpoch).IsGreaterThanOrEqualTo(0);
        }
        finally { release.TrySetResult(Disposition.Complete); }
    }

    [Test]
    public async Task Stop_interrupts_accepted_work_drain_without_waiting_for_late_handlers()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).BuildAsync(token);
        for (var i = 0; i < 2; i++)
            await producer.ProduceAsync(new() { Topic = fixture.Destination, Partition = 0, Key = [(byte)i], Value = [(byte)i] }, token);
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).WithGroupId(fixture.Destination)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).WithAutoOffsetReset(AutoOffsetReset.Earliest).Build();
        native.Assign(new TopicPartition(fixture.Destination, 0));
        await native.InitializeAsync(token);
        var late = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var peer = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = 0;
        await using var adapter = new KafkaConsumer(native, (delivery, _) =>
        {
            received++;
            return new(delivery.Result.Offset == 0 ? late.Task : peer.Task);
        },
            tracing, new() { System = "kafka", Destination = fixture.Destination }, new(),
            options: new() { ConcurrencyPerPartition = 2 }, concurrency: 2);
        await using var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator();
        await Assert.That(await batches.MoveNextAsync()).IsTrue();
        // This entry point enters the same accepted-work drain as a replay. Both handlers
        // are pending when it yields, so stop cannot race ahead of entry into that drain.
        var draining = adapter.ProcessBatchAsync(batches.Current, token).AsTask();
        try
        {
            await Assert.That(received).IsEqualTo(2);
            await Assert.That(draining.IsCompleted).IsFalse();
            await adapter.StopAsync(token);
            // The standalone batch entry point has no running poll pump. A peer completion
            // supplies the wake normally supplied by RunAsync's stop registration.
            peer.SetResult(Disposition.Complete);
            await draining.WaitAsync(TimeSpan.FromSeconds(5), token);
            await Assert.That(late.Task.IsCompleted).IsFalse();
        }
        finally
        {
            peer.TrySetResult(Disposition.Complete);
            late.TrySetResult(Disposition.Complete);
            await draining.WaitAsync(token);
        }
    }

    [Test]
    public async Task Bounded_stop_cancels_active_work_and_fresh_receiver_replays_discarded_queue()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).BuildAsync(token);
        for (var i = 0; i < 8; i++)
            await producer.ProduceAsync(new() { Topic = fixture.Destination, Partition = 0, Key = [1], Value = [(byte)i] }, token);
        var ownership = new KafkaOwnership();
        var native = Native(fixture.Bootstrap, fixture.Destination, ownership);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var replayed = Channel.CreateUnbounded<long>();
        var block = 1;
        await using (var adapter = new KafkaConsumer(native, Receive, tracing,
            new() { System = "kafka", Destination = fixture.Destination }, ownership,
            options: new() { MaxBufferedPerPartition = 4, DrainTimeout = TimeSpan.FromMilliseconds(100) }))
        {
            await adapter.StartAsync(token);
            await entered.Task.WaitAsync(token);
            await WaitUntilAsync(() => native.Paused.Count == 1, token);
            await adapter.StopAsync(token).WaitAsync(TimeSpan.FromSeconds(5), token);
        }
        ownership = new();
        native = Native(fixture.Bootstrap, fixture.Destination, ownership);
        await using var restarted = new KafkaConsumer(native, Receive, tracing,
            new() { System = "kafka", Destination = fixture.Destination }, ownership);
        await restarted.StartAsync(token);
        for (var i = 0; i < 8; i++) await Assert.That(await replayed.Reader.ReadAsync(token)).IsEqualTo((long)i);
        await restarted.StopAsync(token);
        var offsets = await native.GetCommittedOffsetsAsync([new(fixture.Destination, 0)], token);
        await Assert.That(offsets[new(fixture.Destination, 0)].Offset).IsEqualTo(8L);

        async ValueTask<Disposition> Receive(KafkaDelivery delivery, CancellationToken cancellation)
        {
            if (Interlocked.Exchange(ref block, 0) != 0)
            {
                entered.SetResult();
                await Task.Delay(Timeout.Infinite, cancellation);
            }
            replayed.Writer.TryWrite(delivery.Result.Offset);
            return Disposition.Complete;
        }
    }

    [Test]
    public async Task Failed_worker_restart_replays_uncommitted_gap_instead_of_skipping_completed_peers()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).BuildAsync(token);
        for (var i = 0; i < 8; i++)
            await producer.ProduceAsync(new() { Topic = fixture.Destination, Partition = 0, Key = [(byte)i], Value = [(byte)i] }, token);
        var gap = new TaskCompletionSource<Disposition>(TaskCreationOptions.RunContinuationsAsynchronously);
        var peer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ownership = new KafkaOwnership();
        var native = Native(fixture.Bootstrap, fixture.Destination, ownership);
        var options = new KafkaConsumerOptions { ConcurrencyPerPartition = 2 };
        await using (var failed = new KafkaConsumer(native, Receive, tracing,
            new() { System = "kafka", Destination = fixture.Destination }, ownership, options: options, concurrency: 2))
        {
            await failed.StartAsync(token);
            await peer.Task.WaitAsync(token);
            gap.SetException(new ApplicationException("simulated worker crash"));
            await Assert.That(async () => await failed.Completion.WaitAsync(token)).Throws<ApplicationException>();
            var committed = await native.GetCommittedOffsetAsync(new(fixture.Destination, 0), token);
            await Assert.That(committed is null or < 1).IsTrue();
        }
        var replayed = Channel.CreateUnbounded<long>();
        ownership = new();
        native = Native(fixture.Bootstrap, fixture.Destination, ownership);
        await using var restarted = new KafkaConsumer(native, (delivery, _) =>
        {
            replayed.Writer.TryWrite(delivery.Result.Offset);
            return new(Disposition.Complete);
        }, tracing, new() { System = "kafka", Destination = fixture.Destination }, ownership);
        await restarted.StartAsync(token);
        for (var i = 0; i < 8; i++) await Assert.That(await replayed.Reader.ReadAsync(token)).IsEqualTo((long)i);
        await restarted.StopAsync(token);
        await Assert.That(await native.GetCommittedOffsetAsync(new(fixture.Destination, 0), token)).IsEqualTo(8L);

        ValueTask<Disposition> Receive(KafkaDelivery delivery, CancellationToken cancellation)
        {
            if (delivery.Result.Offset == 0) return new(gap.Task);
            peer.TrySetResult();
            return new(Disposition.Complete);
        }
    }

    private static IKafkaConsumer<byte[], byte[]> Native(string bootstrap, string topic, KafkaOwnership ownership, Rebalances? probe = null)
    {
        var builder = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).WithGroupId(topic)
            .WithGroupRemoteAssignor("uniform").WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).SubscribeTo(topic);
        if (probe is not null) builder.AddRebalanceListener(probe);
        return builder.AddRebalanceListener(ownership).Build();
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken token)
    {
        while (!condition()) await Task.Delay(10, token);
    }

    private sealed class Rebalances : IRebalanceListener
    {
        internal readonly Channel<TopicPartition[]> Revoked = Channel.CreateUnbounded<TopicPartition[]>();
        public ValueTask OnPartitionsAssignedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken) => default;
        public ValueTask OnPartitionsRevokedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
        { Revoked.Writer.TryWrite(partitions.ToArray()); return default; }
        public ValueTask OnPartitionsLostAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken) => default;
    }
}
