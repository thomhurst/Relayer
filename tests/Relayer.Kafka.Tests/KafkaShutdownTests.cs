using System.Diagnostics;
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
            const int DrainMarkerOffset = ProbeOffset - 1;

            for (var i = 0; i < ProbeOffset; i++)
            {
                await producer.ProduceAsync(new()
                {
                    Topic = topic,
                    Partition = partition.Partition,
                    Key = [(byte)(graceful && i == DrainMarkerOffset ? 1 : 0)],
                    Value = new byte[] { (byte)i }
                }, token);
            }

            using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var drainMarkerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var commitStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var commitFinished = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var releaseCommit = new ManualResetEventSlim();
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => graceful && ReferenceEquals(source, tracing.Source),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = _ =>
                {
                    commitStarted.TrySetResult();
                    releaseCommit.Wait(token);
                },
                ActivityStopped = activity => commitFinished.TrySetResult(activity),
            };
            ActivitySource.AddActivityListener(listener);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handled = new List<long>();
            var concurrency = graceful ? 2 : 1;
            var options = new PartitionedProcessingOptions
            {
                Ordering = PartitionedProcessingOrder.Key,
                MaxConcurrentHandlersPerPartition = concurrency,
                CommitPolicy = graceful ? PartitionCommitPolicy.CommitCompletedOnRevoke : PartitionCommitPolicy.CommitCompletedPeriodically,
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
                        // The last prefix record runs on a second key, so reaching it proves the whole prefix was queued.
                        await drainMarkerEntered.Task.WaitAsync(token);
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
                            if (await Task.WhenAny(commitStarted.Task, stopping) == stopping)
                            {
                                await stopping;
                            }

                            await Assert.That(commitStarted.Task.IsCompletedSuccessfully).IsTrue()
                                .Because("the completed prefix must reach Dekaf's shutdown commit");
                            await Assert.That(stopping.IsCompleted).IsFalse()
                                .Because("StopAsync must wait for the native commit");

                            // Input has stopped and the prefix has drained. Append the probe before the
                            // commit is sent, so the record is present at shutdown and only restart can read it.
                            await producer.ProduceAsync(new()
                            {
                                Topic = topic,
                                Partition = partition.Partition,
                                Value = [(byte)ProbeOffset],
                            }, token);
                            releaseCommit.Set();

                            await stopping;
                            await Assert.That(commitFinished.Task.IsCompletedSuccessfully).IsTrue()
                                .Because("StopAsync must wait for Dekaf's shutdown commit");
                            var commit = await commitFinished.Task;
                            await Assert.That(commit.Status).IsNotEqualTo(ActivityStatusCode.Error);
                            await Assert.That(commit.GetTagItem("messaging.batch.message_count") as int?).IsEqualTo(ProbeOffset);
                        }
                    }

                    // Read the broker checkpoint independently from the consumer's offset cache.
                    var offsets = await admin.ListConsumerGroupOffsetsAsync(topic, token);
                    var checkpoint = offsets.GetValueOrDefault(partition);

                    if (graceful)
                    {
                        await Assert.That(checkpoint).IsEqualTo((long)ProbeOffset);
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
                    releaseCommit.Set();
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
                lock (handled)
                {
                    handled.Add(delivery.Result.Offset);
                }

                if (graceful && delivery.Result.Offset == DrainMarkerOffset)
                {
                    drainMarkerEntered.SetResult();
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
