using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Microsoft.Extensions.Options;
using Relayer.Pipeline;
using Relayer.Transports;

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
    public async Task Stop_commits_only_the_durable_prefix_and_restart_replays_the_rest(int blocked, bool abort, bool fail)
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
            var graceful = !abort && !fail;
            const int ProbeOffset = 3;
            var recordCount = graceful ? ProbeOffset + 1 : ProbeOffset;

            for (var i = 0; i < recordCount; i++)
            {
                await producer.ProduceAsync(new()
                {
                    Topic = topic,
                    Partition = partition.Partition,
                    Key = [(byte)(i == ProbeOffset ? 1 : 0)],
                    Value = new byte[] { (byte)i }
                }, token);
            }

            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var probeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handled = new List<long>();
            var concurrency = graceful ? 2 : 1;
            var options = new PartitionedProcessingOptions
            {
                Ordering = PartitionedProcessingOrder.Key,
                MaxConcurrentHandlersPerPartition = concurrency,
                CommitPolicy = PartitionCommitPolicy.CommitCompletedPeriodically,
                // Only the forced-stop cases need a short deadline. Graceful stop also waits for the broker commit.
                StopTimeout = TimeSpan.FromSeconds(abort ? 1 : 30),
            };
            long first;

            await using (var adapter = new KafkaConsumer(CreateConsumer, Receive, tracing,
                new()
                {
                    System = "kafka",
                    Destination = topic,
                    ConsumerGroup = topic
                }, options, concurrency))
            {
                try
                {
                    await adapter.StartAsync(token);
                    await entered.Task.WaitAsync(token);

                    if (graceful)
                    {
                        // This later offset runs on a second key, proving the held key's suffix reached the dispatcher.
                        await probeEntered.Task.WaitAsync(token);
                    }

                    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
                    var stopping = adapter.StopAsync(deadline.Token);

                    if (abort)
                    {
                        // The caller's stop wait can end through cancellation or the native drain deadline.
                        deadline.Cancel();

                        try
                        {
                            await ObserveForcedStop(stopping);
                        }
                        catch (OperationCanceledException error) when (error.CancellationToken == deadline.Token
                            && !token.IsCancellationRequested)
                        {
                        }

                        await Assert.That(stopping.IsCompletedSuccessfully).IsFalse();
                        await ObserveForcedStop(adapter.Completion.WaitAsync(token));
                        // Dekaf can report the deadline before the handler observes cancellation.
                        await cancelled.Task.WaitAsync(token);
                    }
                    else
                    {
                        await Assert.That(stopping.IsCompleted).IsFalse();
                        release.SetResult();

                        if (fail)
                        {
                            // Dekaf can surface the handler failure through either wait.
                            await Tolerate(adapter.Completion.WaitAsync(token));
                            await Tolerate(stopping);
                        }
                        else
                        {
                            await stopping;
                        }
                    }

                    await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).WithGroupId(topic)
                        .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
                    await reader.InitializeAsync(token);
                    var checkpoint = await reader.GetCommittedOffsetAsync(partition, token) is { } committed and >= 0 ? committed : 0;

                    if (graceful)
                    {
                        // A graceful drain finishes the held record and the records already queued behind it.
                        await Assert.That(checkpoint).IsEqualTo((long)recordCount);
                        await Assert.That(handled.ToArray()).IsEquivalentTo(new long[] { 0, 1, 2 });
                    }
                    else
                    {
                        // The unfinished or failed record is never committed.
                        await Assert.That(checkpoint).IsLessThanOrEqualTo(blocked);
                        await Assert.That(handled.ToArray()).IsEquivalentTo(Enumerable.Range(0, blocked + 1).Select(i => (long)i).ToArray());
                    }

                    first = checkpoint;
                }
                finally
                {
                    release.TrySetResult();
                }
            }

            await using var restarted = CreateConsumer();
            restarted.Assign(partition);
            await restarted.InitializeAsync(token);
            var replay = new List<long>();

            await using (var batches = restarted.ConsumeBatchAsync(token).GetAsyncEnumerator(token))
            {
                while (replay.Count < recordCount - first && await batches.MoveNextAsync())
                {
                    foreach (var record in batches.Current)
                    {
                        replay.Add(record.Offset);
                    }
                }
            }

            await Assert.That(replay.ToArray()).IsEquivalentTo(Enumerable.Range((int)first, recordCount - (int)first).Select(i => (long)i).ToArray());

            IKafkaConsumer<byte[], byte[]> CreateConsumer()
            {
                return new ConsumerBuilder<byte[], byte[]>()
                                .WithBootstrapServers(broker.Bootstrap).WithGroupId(topic).WithAutoOffsetReset(AutoOffsetReset.Earliest)
                                .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).SubscribeTo(topic).Build();
            }

            async Task ObserveForcedStop(Task stopped)
            {
                try
                {
                    await stopped;
                }
                catch (TimeoutException error) when (error.Message ==
                    $"Partition processor for {partition.Topic}:{partition.Partition} did not stop within {options.StopTimeout}.")
                {
                }
            }

            static async Task Tolerate(Task drained)
            {
                try
                {
                    await drained;
                }
                catch (InvalidOperationException error) when (error.Message == "Durable receive failed.")
                {
                }
            }

            async ValueTask<Disposition> Receive(KafkaDelivery delivery, CancellationToken cancellationToken)
            {
                if (delivery.Result.Offset == ProbeOffset)
                {
                    probeEntered.SetResult();
                    return Disposition.Complete;
                }

                lock (handled)
                {
                    handled.Add(delivery.Result.Offset);
                }

                if (delivery.Result.Offset == blocked)
                {
                    entered.SetResult();

                    try
                    {
                        await release.Task.WaitAsync(cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        cancelled.SetResult();
                        throw;
                    }

                    if (fail)
                    {
                        throw new InvalidOperationException("Durable receive failed.");
                    }
                }

                return Disposition.Complete;
            }
        }
        finally
        {
            await admin.DeleteTopicsAsync([topic], cancellationToken: token);
        }
    }
}
