using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json.Serialization;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Protocol.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

// Kafka-to-Kafka exactly-once over Dekaf's CommitCompletedInTransaction policy, read back with read_committed.
[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaExactlyOnceTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Handler_sends_and_dead_letter_copies_commit_once_with_the_consumed_offsets(bool observe)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-commit", 1, token);
        await ProduceAsync(topics.Input, 1, token, Enumerable.Range(0, 10).ToArray());
        var control = new ExactlyOnceControl { Poison = 3 };

        using (var host = BuildHost(topics, control, TimeSpan.FromMinutes(5), observe: observe))
        {
            await host.StartAsync(token);

            // Wait until the broker has the copy and all successful handlers have reached their send.
            while (control.Handled.Count != 9 ||
                (await ReadOutputAsync(topics.DeadLetter, 1, token, IsolationLevel.ReadUncommitted)).Count == 0)
            {
                await Task.Delay(50, token);
            }

            await Assert.That(await CommittedAsync(topics.Input, 0, token)).IsNull();
            await Assert.That(await ReadOutputAsync(topics.DeadLetter, 1, token)).IsEmpty();
            await host.StopAsync(token);
            await Assert.That(await CommittedAsync(topics.Input, 0, token)).IsEqualTo(10L);

            if (observe)
            {
                var harness = host.Services.GetRequiredService<RelayerHarness>();
                await Assert.That(harness.GetDispatched<ExactlyOnceOutput>().Select(message => message.Number))
                    .IsEquivalentTo(Enumerable.Range(0, 10).Where(number => number != 3));
                await Assert.That(harness.Dispatched.Count(message => message.Destination == topics.DeadLetter)).IsEqualTo(1);
            }
        }

        var output = await ReadOutputAsync(topics.Output, 1, token);
        await Assert.That(output.Order()).IsEquivalentTo(Enumerable.Range(0, 10).Where(number => number != 3));
        await Assert.That(await ReadOutputAsync(topics.DeadLetter, 1, token)).IsEquivalentTo([3]);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task A_failed_run_aborts_its_output_and_the_next_run_produces_it_once(bool observe)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-abort", 1, token);
        await ProduceAsync(topics.Input, 1, token, Enumerable.Range(0, 10).ToArray());
        // The dead-letter copy of record 5 cannot be written, so the run fails inside its first transaction.
        var failing = new ExactlyOnceControl { Poison = 5 };

        using (var host = BuildHost(topics, failing, TimeSpan.FromMinutes(5), missingDeadLetter: true, observe: observe))
        {
            await host.StartAsync(token);
            var receiver = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events").Consumers[0];
            // The copy times out, so the run ends cancelled rather than faulted; either is a failed run.
            try
            {
                await receiver.Completion.WaitAsync(token);
            }
            catch (Exception) when (!token.IsCancellationRequested)
            {
            }

            await Assert.That(receiver.Completion.IsCompletedSuccessfully).IsFalse();
            await Assert.That(failing.Handled.Count).IsEqualTo(5);
        }

        await Assert.That(await CommittedAsync(topics.Input, 0, token)).IsNull();
        await Assert.That(await ReadOutputAsync(topics.Output, 1, token)).IsEmpty();

        var recovered = new ExactlyOnceControl();

        using (var host = BuildHost(topics, recovered, TimeSpan.FromMilliseconds(200)))
        {
            await host.StartAsync(token);

            while (await CommittedAsync(topics.Input, 0, token) != 10)
            {
                await Task.Delay(50, token);
            }

            await host.StopAsync(token);
        }

        var output = await ReadOutputAsync(topics.Output, 1, token);
        await Assert.That(output.Order()).IsEquivalentTo(Enumerable.Range(0, 10));
    }

    [Test]
    public async Task Rebalance_and_shutdown_commit_each_output_once()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-rebalance", 2, token);
        var produced = 0;
        var firstControl = new ExactlyOnceControl();
        var secondControl = new ExactlyOnceControl();
        using var first = BuildHost(topics, firstControl, TimeSpan.FromSeconds(1));
        using var second = BuildHost(topics, secondControl, TimeSpan.FromSeconds(1));
        await first.StartAsync(token);
        await ProduceBatchAsync();

        while (firstControl.Handled.IsEmpty)
        {
            await Task.Delay(20, token);
        }
        // Joining revokes a partition from the first member while its transaction is open. Keep producing until the
        // second member owns a partition and handles a record.
        await second.StartAsync(token);

        while (secondControl.Handled.IsEmpty)
        {
            await ProduceBatchAsync();
        }
        // Stopping commits the first member's transaction and hands its partition to the second member.
        await first.StopAsync(token);
        await ProduceBatchAsync();

        while (await CommittedAsync(topics.Input, 0, token) + await CommittedAsync(topics.Input, 1, token) != produced)
        {
            await Task.Delay(50, token);
        }

        await second.StopAsync(token);
        var output = await ReadOutputAsync(topics.Output, 2, token);
        await Assert.That(output.Order()).IsEquivalentTo(Enumerable.Range(0, produced));

        async Task ProduceBatchAsync()
        {
            await ProduceAsync(topics.Input, 2, token, Enumerable.Range(produced, 20).ToArray());
            produced += 20;
            await Task.Delay(100, token);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task A_process_crash_on_either_side_of_commit_preserves_exactly_once_output(bool committed)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-crash", 1, token);
        await ProduceAsync(topics.Input, 1, token, [0]);

        using (var probe = StartCrashProbe(topics, committed))
        {
            try
            {
                // Broker observations establish the boundary before killing the process.
                if (committed)
                {
                    while (await CommittedAsync(topics.Input, 0, token) != 1)
                    {
                        await WaitForProbeAsync(probe, token);
                    }
                }
                else
                {
                    while ((await ReadOutputAsync(topics.Output, 1, token, IsolationLevel.ReadUncommitted)).Count == 0)
                    {
                        await WaitForProbeAsync(probe, token);
                    }

                    await Assert.That(await CommittedAsync(topics.Input, 0, token)).IsNull();
                }

                await Assert.That(probe.HasExited).IsFalse();
            }
            finally
            {
                if (!probe.HasExited)
                {
                    probe.Kill(entireProcessTree: true);
                    await probe.WaitForExitAsync();
                }
            }
        }

        await Assert.That(await CommittedAsync(topics.Input, 0, token)).IsEqualTo(committed ? 1L : (long?)null);
        await ProduceAsync(topics.Input, 1, token, [1]);
        var recovered = new ExactlyOnceControl();
        // Reuse the crashed instance's ID so Dekaf fences its producer and clears any open transaction.
        using (var host = BuildHost(topics, recovered, TimeSpan.FromMilliseconds(200),
            transactionalId: topics.Input + KafkaTransactionProbe.TransactionalIdSuffix))
        {
            await host.StartAsync(token);

            while (await CommittedAsync(topics.Input, 0, token) != 2)
            {
                await Task.Delay(50, token);
            }

            await host.StopAsync(token);
        }

        await Assert.That(recovered.Handled.Order()).IsEquivalentTo(committed ? new[] { 1 } : [0, 1]);
        await Assert.That(await ReadOutputAsync(topics.Output, 1, token)).IsEquivalentTo([0, 1]);
    }

    [Test]
    public async Task A_cancelled_handler_leaves_no_output_or_offset_before_replay()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-cancel", 1, token);
        await ProduceAsync(topics.Input, 1, token, [0]);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopping = new ExactlyOnceControl { Block = entered };

        using (var host = BuildHost(topics, stopping, TimeSpan.FromMinutes(5)))
        {
            await host.StartAsync(token);
            await entered.Task.WaitAsync(token);
            // Dekaf aborts and faults the final commit when a lane has unfinished records.
            await Assert.That(() => host.StopAsync(token)).Throws<InvalidOperationException>()
                .WithMessageContaining("stopped before completing the records it read");
        }

        await Assert.That(stopping.Handled).IsEmpty();
        await Assert.That(await CommittedAsync(topics.Input, 0, token)).IsNull();
        await Assert.That(await ReadOutputAsync(topics.Output, 1, token)).IsEmpty();

        using (var host = BuildHost(topics, new(), TimeSpan.FromMilliseconds(200)))
        {
            await host.StartAsync(token);

            while (await CommittedAsync(topics.Input, 0, token) != 1)
            {
                await Task.Delay(50, token);
            }

            await host.StopAsync(token);
        }

        await Assert.That(await ReadOutputAsync(topics.Output, 1, token)).IsEquivalentTo([0]);
    }

    private static Task WaitForProbeAsync(Process probe, CancellationToken token)
    {
        if (probe.HasExited)
        {
            throw new InvalidOperationException($"Kafka crash probe exited early with code {probe.ExitCode}.");
        }

        return Task.Delay(50, token);
    }

    private Process StartCrashProbe(Topics topics, bool committed)
    {
        var assembly = typeof(KafkaExactlyOnceTests).Assembly.Location;
        var start = new ProcessStartInfo("dotnet") { UseShellExecute = false, CreateNoWindow = true };

        foreach (var arg in new[]
        {
            "exec", "--runtimeconfig", Path.ChangeExtension(assembly, ".runtimeconfig.json"),
            "--depsfile", Path.ChangeExtension(assembly, ".deps.json"), typeof(CrashProbeMarker).Assembly.Location,
            "kafka-transaction", broker.Bootstrap, topics.Input, topics.Output, committed.ToString(),
        })
        {
            start.ArgumentList.Add(arg);
        }

        return Process.Start(start) ?? throw new InvalidOperationException("Could not start the Kafka crash probe.");
    }

    [Test]
    public async Task Consumer_callback_cannot_add_a_topic_to_the_transactional_source()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-source", 1, token);
        await using var archive = await TopicsAsync("eos-archive", 1, token);
        await ProduceAsync(topics.Input, 1, token, [0]);
        await ProduceAsync(archive.Input, 1, token, [1]);
        var listener = new AssignedTopics();
        using var host = BuildHost(topics, new(), TimeSpan.FromMilliseconds(200),
            configureConsumer: native => native.SubscribeTo(archive.Input).AddRebalanceListener(listener));

        await host.StartAsync(token);

        while (await CommittedAsync(topics.Input, 0, token) != 1)
        {
            await Task.Delay(50, token);
        }

        await host.StopAsync(token);
        await Assert.That(await ReadOutputAsync(topics.Output, 1, token)).IsEquivalentTo([0]);
        await Assert.That(listener.Topics.Distinct()).IsEquivalentTo([topics.Input]);
    }

    [Test]
    public async Task Manual_retry_reports_the_supported_policy_and_can_leave_the_send_transaction_intact()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-manual-retry", 1, token);
        await ProduceAsync(topics.Input, 1, token, [0]);
        var scheduler = new CountingScheduler();
        InvalidOperationException? error = null;
        var control = new ExactlyOnceControl
        {
            BeforeSend = context =>
            {
                try
                {
                    context.Features.GetRequired<ISettlementFeature>().Retry(TimeSpan.FromSeconds(1));
                }
                catch (InvalidOperationException exception)
                {
                    error = exception;
                }
            }
        };
        using var host = BuildHost(topics, control, TimeSpan.FromMilliseconds(200),
            configureServices: services => services.AddSingleton<IFailureScheduler>(scheduler));

        await host.StartAsync(token);

        while (await CommittedAsync(topics.Input, 0, token) != 1)
        {
            await Task.Delay(50, token);
        }

        await host.StopAsync(token);
        await Assert.That(error?.Message).Contains("Kafka does not support manual redelivery");
        await Assert.That(error?.Message).Contains("RetryDelays");
        await Assert.That(scheduler.Calls).IsEqualTo(0);
        await Assert.That(await ReadOutputAsync(topics.Output, 1, token)).IsEquivalentTo([0]);
    }

    [Test]
    public async Task A_database_inbox_is_rejected_at_startup()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-inbox", 1, token);
        using var host = BuildHost(topics, new(), TimeSpan.FromMilliseconds(200), inbox: true);
        await Assert.That(() => host.StartAsync(token)).Throws<NotSupportedException>().WithMessageContaining("inbox");
    }

    private IHost BuildHost(Topics topics, ExactlyOnceControl control, TimeSpan commitInterval, bool missingDeadLetter = false,
        bool inbox = false, string? transactionalId = null, bool observe = false,
        Action<ConsumerBuilder<byte[], byte[]>>? configureConsumer = null, Action<IServiceCollection>? configureServices = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<HostOptions>(options => options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore);
        builder.Services.AddSingleton(control);
        configureServices?.Invoke(builder.Services);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(ExactlyOnceJson.Default);
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(broker.Bootstrap);
                    options.ConfigureConsumer = native =>
                    {
                        native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                        configureConsumer?.Invoke(native);
                    };
                    options.ConfigureProducer = native => native.WithAllowAutoCreateTopics(false);
                });
                kafka.Send<ExactlyOnceOutput>(topics.Output);
                kafka.Consume(topics.Input, consumer =>
                {
                    consumer.Options.Group = topics.Input;
                    consumer.Options.TransactionalId = transactionalId;
                    consumer.Options.Processing = new()
                    {
                        Ordering = PartitionedProcessingOrder.Key,
                        CommitPolicy = PartitionCommitPolicy.CommitCompletedInTransaction,
                        CommitInterval = commitInterval,
                        StopPolicy = control.Block is null ? PartitionStopPolicy.Drain : PartitionStopPolicy.Cancel,
                    };
                    consumer.Failures(new()
                    {
                        RetryDelays = [],
                        Jitter = 0,
                        DeadLetterEndpoint = missingDeadLetter ? topics.Input + ".missing" : topics.DeadLetter,
                        CopyTimeout = missingDeadLetter ? TimeSpan.FromSeconds(2) : new FailureOptions().CopyTimeout,
                    });

                    if (inbox)
                    {
                        consumer.ConfigurePipeline(pipeline => pipeline.Use(new InboxBoundary()));
                    }

                    consumer.Handler<ExactlyOnceHandler>();
                });
            });
        });

        if (observe)
        {
            builder.Services.AddRelayerHarness();
        }

        return builder.Build();
    }

    private async Task<long?> CommittedAsync(string topic, int partition, CancellationToken token)
    {
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).WithGroupId(topic)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await reader.InitializeAsync(token);
        var offset = await reader.GetCommittedOffsetAsync(new(topic, partition), token);
        return offset is null or < 0 ? null : offset;
    }

    // Reads up to each partition's last stable offset by default.
    private async Task<List<int>> ReadOutputAsync(string topic, int partitions, CancellationToken token,
        IsolationLevel isolation = IsolationLevel.ReadCommitted)
    {
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
            .WithIsolationLevel(isolation).WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithPartitionEof(true).Build();
        reader.Assign(Enumerable.Range(0, partitions).Select(partition => new TopicPartition(topic, partition)).ToArray());
        await reader.InitializeAsync(token);
        var numbers = new List<int>();
        var ended = new HashSet<int>();

        await foreach (var record in reader.ConsumeAsync(token))
        {
            if (record.IsPartitionEof)
            {
                if (ended.Add(record.Partition) && ended.Count == partitions)
                {
                    break;
                }

                continue;
            }

            numbers.Add(int.Parse(Encoding.UTF8.GetString(record.Value).Split(':')[1].TrimEnd('}')));
        }

        return numbers;
    }

    private async Task ProduceAsync(string topic, int partitions, CancellationToken token, int[] numbers)
    {
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap).BuildAsync(token);

        foreach (var number in numbers)
        {
            await producer.ProduceAsync(new()
            {
                Topic = topic,
                Partition = number % partitions,
                Key = BitConverter.GetBytes(number),
                Value = Encoding.UTF8.GetBytes($"{{\"Number\":{number}}}"),
                Headers = new Dekaf.Serialization.Headers().Add("ce_id", "input-" + number).Add("ce_type", KafkaTransactionProbe.InputName + ";v=1"),
            }, token);
        }
    }

    private async Task<Topics> TopicsAsync(string name, int partitions, CancellationToken token)
    {
        await broker.Started.WaitAsync(token);
        var input = TestContext.Current!.Isolation.GetIsolatedName(name).ToLowerInvariant();
        var admin = new AdminClientBuilder().WithBootstrapServers(broker.Bootstrap).Build();
        var topics = new Topics(input, admin);
        await admin.CreateTopicsAsync([new() { Name = topics.Input, NumPartitions = partitions, ReplicationFactor = 1 },
            new() { Name = topics.Output, NumPartitions = partitions, ReplicationFactor = 1 },
            new() { Name = topics.DeadLetter, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        return topics;
    }

    private sealed class AssignedTopics : IRebalanceListener
    {
        internal ConcurrentQueue<string> Topics
        {
            get;
        } = new();

        public ValueTask OnPartitionsAssignedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
        {
            foreach (var partition in partitions)
            {
                Topics.Enqueue(partition.Topic);
            }

            return default;
        }

        public ValueTask OnPartitionsRevokedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
        {
            return default;
        }

        public ValueTask OnPartitionsLostAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
        {
            return default;
        }
    }

    private sealed class CountingScheduler : IFailureScheduler
    {
        private int _calls;

        internal int Calls => Volatile.Read(ref _calls);

        public ValueTask ScheduleAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _calls);
            return default;
        }
    }

    private sealed class Topics(string input, IAdminClient admin) : IAsyncDisposable
    {
        internal string Input
        {
            get;
        } = input;

        internal string Output
        {
            get;
        } = input + ".output";

        internal string DeadLetter
        {
            get;
        } = input + ".dead-letter";

        public async ValueTask DisposeAsync()
        {
            try
            {
                await admin.DeleteTopicsAsync([Input, Output, DeadLetter]);
            }
            finally
            {
                await admin.DisposeAsync();
            }
        }
    }
}

[Message(KafkaTransactionProbe.InputName)]
public sealed record ExactlyOnceInput(int Number);

[Message("tests.kafka.eos.output")]
public sealed record ExactlyOnceOutput(int Number);

public sealed class ExactlyOnceControl
{
    internal int? Poison
    {
        get; init;
    }

    internal TaskCompletionSource? Block
    {
        get; init;
    }

    internal Action<MessageContext>? BeforeSend
    {
        get; init;
    }

    internal ConcurrentQueue<int> Handled
    {
        get;
    } = new();
}

public sealed class ExactlyOnceHandler(ExactlyOnceControl control, ISender sender) : IHandler<ExactlyOnceInput>
{
    public async ValueTask HandleAsync(ExactlyOnceInput message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Number == control.Poison)
        {
            throw new InvalidOperationException("poison record");
        }

        control.BeforeSend?.Invoke(context);
        await sender.SendAsync(new ExactlyOnceOutput(message.Number), cancellationToken);

        if (control.Block is { } entered)
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }

        control.Handled.Enqueue(message.Number);
    }
}

[JsonSerializable(typeof(ExactlyOnceInput))]
[JsonSerializable(typeof(ExactlyOnceOutput))]
internal partial class ExactlyOnceJson : JsonSerializerContext;

// Stands in for a database consumer boundary that deduplicates through an inbox.
internal sealed class InboxBoundary : IConsumeTransactionMiddleware, IConsumeInbox
{
    public bool Inbox => true;

    public ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
    {
        return next(context);
    }
}
