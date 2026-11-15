using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Serialization;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Protocol.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

// Kafka-to-Kafka exactly-once over Dekaf's CommitCompletedInTransaction policy, read back with read_committed.
[ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
public class KafkaExactlyOnceTests(KafkaHeaderSnapshotBroker broker)
{
    [Test]
    public async Task Handler_sends_and_dead_letter_copies_commit_once_with_the_consumed_offsets()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-commit", 1, token);
        await ProduceAsync(topics.Input, 1, token, Enumerable.Range(0, 10).ToArray());
        var control = new ExactlyOnceControl { Poison = 3 };
        using (var host = BuildHost(topics, control, TimeSpan.FromMilliseconds(200)))
        {
            await host.StartAsync(token);
            while (await CommittedAsync(topics.Input, 0, token) != 10) await Task.Delay(50, token);
            await host.StopAsync(token);
        }
        var output = await ReadCommittedAsync(topics.Output, 1, token);
        await Assert.That(output.Order()).IsEquivalentTo(Enumerable.Range(0, 10).Where(number => number != 3));
        await Assert.That(await ReadCommittedAsync(topics.DeadLetter, 1, token)).IsEquivalentTo([3]);
    }

    [Test]
    public async Task A_failed_run_aborts_its_output_and_the_next_run_produces_it_once()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await using var topics = await TopicsAsync("eos-abort", 1, token);
        await ProduceAsync(topics.Input, 1, token, Enumerable.Range(0, 10).ToArray());
        // The dead-letter copy of record 5 cannot be written, so the run fails inside its first transaction.
        var failing = new ExactlyOnceControl { Poison = 5 };
        using (var host = BuildHost(topics, failing, TimeSpan.FromMinutes(5), missingDeadLetter: true))
        {
            await host.StartAsync(token);
            var receiver = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events").Consumers[0];
            // The copy times out, so the run ends cancelled rather than faulted; either is a failed run.
            try { await receiver.Completion.WaitAsync(token); }
            catch (Exception) when (!token.IsCancellationRequested) { }
            await Assert.That(receiver.Completion.IsCompletedSuccessfully).IsFalse();
            await Assert.That(failing.Handled.Count).IsEqualTo(5);
        }
        await Assert.That(await CommittedAsync(topics.Input, 0, token)).IsNull();
        await Assert.That(await ReadCommittedAsync(topics.Output, 1, token)).IsEmpty();

        var recovered = new ExactlyOnceControl();
        using (var host = BuildHost(topics, recovered, TimeSpan.FromMilliseconds(200)))
        {
            await host.StartAsync(token);
            while (await CommittedAsync(topics.Input, 0, token) != 10) await Task.Delay(50, token);
            await host.StopAsync(token);
        }
        var output = await ReadCommittedAsync(topics.Output, 1, token);
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
        while (firstControl.Handled.IsEmpty) await Task.Delay(20, token);
        // Joining revokes a partition from the first member while its transaction is open. Keep producing until the
        // second member owns a partition and handles a record.
        await second.StartAsync(token);
        while (secondControl.Handled.IsEmpty) await ProduceBatchAsync();
        // Stopping commits the first member's transaction and hands its partition to the second member.
        await first.StopAsync(token);
        await ProduceBatchAsync();
        while (await CommittedAsync(topics.Input, 0, token) + await CommittedAsync(topics.Input, 1, token) != produced)
            await Task.Delay(50, token);
        await second.StopAsync(token);
        var output = await ReadCommittedAsync(topics.Output, 2, token);
        await Assert.That(output.Order()).IsEquivalentTo(Enumerable.Range(0, produced));

        async Task ProduceBatchAsync()
        {
            await ProduceAsync(topics.Input, 2, token, Enumerable.Range(produced, 20).ToArray());
            produced += 20;
            await Task.Delay(100, token);
        }
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
        bool inbox = false)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<HostOptions>(options => options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore);
        builder.Services.AddSingleton(control);
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
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                    options.ConfigureProducer = native => native.WithAllowAutoCreateTopics(false);
                });
                kafka.Send<ExactlyOnceOutput>(topics.Output);
                kafka.Consume(topics.Input, consumer =>
                {
                    consumer.Options.Group = topics.Input;
                    consumer.Options.Processing = new()
                    {
                        Ordering = PartitionedProcessingOrder.Key,
                        CommitPolicy = PartitionCommitPolicy.CommitCompletedInTransaction,
                        CommitInterval = commitInterval,
                    };
                    consumer.Failures(new()
                    {
                        RetryDelays = [], Jitter = 0,
                        DeadLetterEndpoint = missingDeadLetter ? topics.Input + ".missing" : topics.DeadLetter,
                        CopyTimeout = missingDeadLetter ? TimeSpan.FromSeconds(2) : new FailureOptions().CopyTimeout,
                    });
                    if (inbox) consumer.ConfigurePipeline(pipeline => pipeline.Use(new InboxBoundary()));
                    consumer.Handler<ExactlyOnceHandler>();
                });
            });
        });
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

    // Reads every committed record up to each partition's last stable offset.
    private async Task<List<int>> ReadCommittedAsync(string topic, int partitions, CancellationToken token)
    {
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(broker.Bootstrap)
            .WithIsolationLevel(IsolationLevel.ReadCommitted).WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithPartitionEof(true).Build();
        reader.Assign(Enumerable.Range(0, partitions).Select(partition => new TopicPartition(topic, partition)).ToArray());
        await reader.InitializeAsync(token);
        var numbers = new List<int>();
        var ended = new HashSet<int>();
        await foreach (var record in reader.ConsumeAsync(token))
        {
            if (record.IsPartitionEof)
            {
                if (ended.Add(record.Partition) && ended.Count == partitions) break;
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
            await producer.ProduceAsync(new()
            {
                Topic = topic, Partition = number % partitions, Key = BitConverter.GetBytes(number),
                Value = Encoding.UTF8.GetBytes($"{{\"Number\":{number}}}"),
                Headers = new Dekaf.Serialization.Headers().Add("ce_id", "input-" + number).Add("ce_type", "tests.kafka.eos.input;v=1"),
            }, token);
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

    private sealed class Topics(string input, IAdminClient admin) : IAsyncDisposable
    {
        internal string Input { get; } = input;
        internal string Output { get; } = input + ".output";
        internal string DeadLetter { get; } = input + ".dead-letter";
        public async ValueTask DisposeAsync()
        {
            try { await admin.DeleteTopicsAsync([Input, Output, DeadLetter]); }
            finally { await admin.DisposeAsync(); }
        }
    }
}

[Message("tests.kafka.eos.input")]
public sealed record ExactlyOnceInput(int Number);
[Message("tests.kafka.eos.output")]
public sealed record ExactlyOnceOutput(int Number);

public sealed class ExactlyOnceControl
{
    internal int? Poison { get; init; }
    internal ConcurrentQueue<int> Handled { get; } = new();
}

public sealed class ExactlyOnceHandler(ExactlyOnceControl control, ISender sender) : IHandler<ExactlyOnceInput>
{
    public async ValueTask HandleAsync(ExactlyOnceInput message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Number == control.Poison) throw new InvalidOperationException("poison record");
        await sender.SendAsync(new ExactlyOnceOutput(message.Number), cancellationToken);
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

    public ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next) => next(context);
}
