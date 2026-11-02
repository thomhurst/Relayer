using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Dekaf;
using Dekaf.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaReceiveTests
{
    [Test]
    [Arguments(1)]
    [Arguments(3)]
    public async Task Retained_partition_replays_invalidated_batch_before_committing(int invalidateAfter)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await fixture.Transport.StartAsync(token);
        for (var i = 0; i < 3; i++) await fixture.Transport.SendAsync(Message(fixture.Destination, i), token);
        var partition = new TopicPartition(fixture.Destination, 0);
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        native.Assign(partition);
        var ownership = new KafkaOwnership();
        var handled = new List<long>();
        await using var adapter = new KafkaConsumer(native, Receive, tracing,
            new() { System = "kafka", Destination = fixture.Destination, ConsumerGroup = fixture.Destination }, ownership);
        await native.InitializeAsync(token);
        await using var batches = native.ConsumeBatchAsync(token).GetAsyncEnumerator(token);
        await Assert.That(await batches.MoveNextAsync()).IsTrue();
        await Assert.That(await adapter.ProcessBatchAsync(batches.Current, token)).IsEqualTo(0);
        await Assert.That(handled.Count).IsEqualTo(invalidateAfter);
        await Assert.That(native.GetPosition(partition)).IsEqualTo(0L);
        var committed = await native.GetCommittedOffsetAsync(partition, token);
        await Assert.That(committed is null or < 0).IsTrue();
        handled.Clear();
        var completed = 0;
        while (completed < 3 && await batches.MoveNextAsync())
            completed += await adapter.ProcessBatchAsync(batches.Current, token);
        await Assert.That(handled.ToArray()).IsEquivalentTo(new long[] { 0, 1, 2 });
        await Assert.That(await native.GetCommittedOffsetAsync(partition, token)).IsEqualTo(3L);

        async ValueTask<Disposition> Receive(KafkaDelivery delivery, CancellationToken cancellationToken)
        {
            handled.Add(delivery.Result.Offset);
            if (handled.Count == invalidateAfter && ownership.Version == 0)
                await ownership.OnPartitionsAssignedAsync([partition], cancellationToken);
            return Disposition.Complete;
        }
    }

    [Test]
    public async Task Partial_start_failure_disposes_consumers_already_started()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        fixture.RegisterConsumer(static (_, _) => new(Disposition.Complete));
        fixture.RegisterConsumer(static (_, _) => new(Disposition.Complete));
        var transport = (KafkaProducerTransport)fixture.Transport;
        // Simulate an unrestartable second consumer without relying on broker timing.
        await transport.Consumers[1].StopAsync(new CancellationToken(canceled: true));
        await Assert.That(async () => await transport.StartAsync(token)).Throws<InvalidOperationException>()
            .WithMessageContaining("failed or aborted");
        await Assert.That(transport.Consumers[0].Completion.IsCompleted).IsTrue();
        await Assert.That(async () => await transport.StartAsync(token)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Core_retry_durable_dead_letter_and_native_commits_preserve_creation_context()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var control = new KafkaReceiveControl();
        var spans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is "Relayer" or "Dekaf",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        using var host = BuildHost(fixture, control);
        await host.StartAsync(token);
        await fixture.Transport.StartAsync(token);
        var source = fixture.Destination;
        await fixture.Transport.SendAsync(Message(source, 1), token);
        await control.Entered.Task.WaitAsync(token);
        await using var offsets = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(source).WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await offsets.InitializeAsync(token);
        var before = await offsets.GetCommittedOffsetAsync(new(source, 0), token);
        await Assert.That(before is null or < 0).IsTrue();
        control.Release.SetResult();
        await Assert.That(await control.Handled.Reader.ReadAsync(token)).IsEqualTo(1);
        var poison = Message(source, 2);
        await fixture.Transport.SendAsync(new(source, poison.Body, poison.Headers, poison.OrderingKey,
            options: new SendOptions().ConfigureNative<Dekaf.Serialization.Headers>(static headers =>
                headers.Add(new Dekaf.Serialization.Header("nullable", (byte[]?)null)).Add("nullable", "second"))), token);
        var tombstone = Message(source, 4);
        await fixture.Transport.SendAsync(new(source, default, tombstone.Headers, tombstone.OrderingKey,
            isTombstone: true), token);
        // Bypass Relayer's send marker handling to simulate an untrusted source header.
        await using var rawProducer = new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).Build();
        await rawProducer.InitializeAsync(token);
        await rawProducer.ProduceAsync(new()
        {
            Topic = source, Key = "customer-1"u8.ToArray(), Value = [],
            Headers = new Dekaf.Serialization.Headers().Add("ce_id", "empty-with-marker")
                .Add("ce_type", "tests.kafka.receive;v=1").Add("traceparent", Creation)
                .Add(KafkaSendState.TombstoneHeader, "true")
                .Add(new Dekaf.Serialization.Header(KafkaSendState.TombstoneHeader, (byte[]?)null)),
        }, token);
        await fixture.Transport.SendAsync(Message(source, 3), token);
        await Assert.That(await control.Handled.Reader.ReadAsync(token)).IsEqualTo(3);
        await host.StopAsync(token);
        var service = host.Services.GetServices<IHostedService>().OfType<KafkaHostedService>().Single();
        await Assert.That(async () => await service.StartAsync(token)).Throws<InvalidOperationException>()
            .WithMessageContaining("only start once");
        await Assert.That(host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events").Consumers.Count).IsEqualTo(1);
        await Assert.That(control.FirstAttempts).IsEqualTo(2);
        await Assert.That(control.LastFeature.ConsumeResult.Topic).IsNull();
        await Assert.That(await offsets.GetCommittedOffsetAsync(new(source, 0), token)).IsEqualTo(5L);

        await using var deadReader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithOffsetCommitMode(OffsetCommitMode.Manual).Build();
        deadReader.Assign(new TopicPartition(source + ".dead-letter", 0));
        await deadReader.InitializeAsync(token);
        var dead = (await deadReader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(Encoding.UTF8.GetString(dead.Value)).IsEqualTo("{\"Number\":2}");
        await Assert.That(Encoding.UTF8.GetString(dead.Key!)).IsEqualTo("customer-1");
        await Assert.That(dead.Headers.First(header => header.Key == "traceparent").GetValueAsString()).IsEqualTo(Creation);
        await Assert.That(dead.Headers.First(header => header.Key == "tracestate").GetValueAsString()).IsEqualTo("tenant=original");
        await Assert.That(dead.Headers.First(header => header.Key == FailureHeaders.History).GetValueAsString()).Contains("poison record");
        var nullable = dead.Headers.Where(header => header.Key == "nullable").ToArray();
        await Assert.That(nullable.Length).IsEqualTo(2);
        await Assert.That(nullable[0].IsValueNull).IsTrue();
        await Assert.That(nullable[1].GetValueAsString()).IsEqualTo("second");
        var deadTombstone = (await deadReader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(deadTombstone.Value).IsNull();
        await Assert.That(deadTombstone.Headers.First(header => header.Key == "traceparent").GetValueAsString()).IsEqualTo(Creation);
        await Assert.That(deadTombstone.Headers.First(header => header.Key == "tracestate").GetValueAsString()).IsEqualTo("tenant=original");
        var deadEmpty = (await deadReader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(deadEmpty.Value).IsNotNull();
        await Assert.That(deadEmpty.Value.Length).IsEqualTo(0);
        await Assert.That(deadEmpty.Headers.Any(header => header.Key == KafkaSendState.TombstoneHeader)).IsFalse();
        var processes = spans.Where(span => span.Source.Name == "Relayer" && span.DisplayName == "process " + source).ToArray();
        await Assert.That(processes.Length).IsEqualTo(5);
        await Assert.That(processes.Count(span => Equals(span.GetTagItem("messaging.kafka.message.tombstone"), true))).IsEqualTo(1);
        await Assert.That(processes.All(span => Equals(span.GetTagItem("messaging.consumer.group.name"), source))).IsTrue();
        await Assert.That(processes.All(span => Equals(span.GetTagItem("messaging.destination.partition.id"), "0"))).IsTrue();
        await Assert.That(processes.All(span => Equals(span.GetTagItem("messaging.kafka.message.key"), "customer-1"))).IsTrue();
        await Assert.That(processes.All(span => span.GetTagItem("messaging.kafka.offset") is long)).IsTrue();
        var commits = spans.Where(span => span.Source.Name == "Relayer" && span.DisplayName == "commit " + source).ToArray();
        await Assert.That(commits.Length).IsGreaterThan(0);
        await Assert.That(commits.All(span => span.GetTagItem("messaging.batch.message_count") is int &&
            processes.All(process => process.SpanId != span.ParentSpanId))).IsTrue();
        var copySends = spans.Where(span => span.Source.Name == "Relayer" && span.DisplayName == "send " + source + ".dead-letter").ToArray();
        await Assert.That(copySends.All(copy => spans.Any(span => span.Source.Name == "Dekaf" && span.ParentSpanId == copy.SpanId))).IsTrue();
    }

    [Test]
    public async Task Failed_dead_letter_copy_stops_receiver_without_committing_original()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        using var host = BuildHost(fixture, new(), missingDeadLetter: true);
        await host.StartAsync(token);
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 2), token);
        var transport = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events");
        await Assert.That(async () => await transport.Consumers[0].Completion.WaitAsync(token)).Throws<OperationCanceledException>();
        await using var offsets = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await offsets.InitializeAsync(token);
        var committed = await offsets.GetCommittedOffsetAsync(new(fixture.Destination, 0), token);
        await Assert.That(committed is null or < 0).IsTrue();
    }

    [Test]
    public async Task Failed_consumer_reaches_host_while_sibling_is_running_and_shutdown_stops_both()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var control = new KafkaReceiveControl();
        using var host = BuildHost(fixture, control, missingDeadLetter: true, healthySibling: true);
        await host.StartAsync(token);
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 3), token);
        await Assert.That(await control.Handled.Reader.ReadAsync(token)).IsEqualTo(3);
        await Assert.That(await control.Handled.Reader.ReadAsync(token)).IsEqualTo(3);
        var transport = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events");
        var service = host.Services.GetServices<IHostedService>().OfType<KafkaHostedService>().Single();
        await fixture.Transport.SendAsync(Message(fixture.Destination, 2), token);
        await Assert.That(async () => await transport.Consumers[0].Completion.WaitAsync(token)).Throws<OperationCanceledException>();
        await Assert.That(transport.Consumers[1].Completion.IsCompleted).IsFalse();
        await Assert.That(async () => await service.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(10), token))
            .Throws<OperationCanceledException>();
        await Assert.That(async () => await host.StopAsync(token)).Throws<OperationCanceledException>();
        await Assert.That(transport.Consumers[0].Completion.IsCompleted).IsTrue();
        await Assert.That(transport.Consumers[1].Completion.IsCompletedSuccessfully).IsTrue();
        await Assert.That(timeout.IsCancellationRequested).IsFalse();
    }

    private const string Creation = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
    private static OutgoingMessage Message(string topic, int number) => new(topic, Encoding.UTF8.GetBytes($"{{\"Number\":{number}}}"),
        new KeyValuePair<string, WireString>[] { new("ce_id", new("record-" + number)), new("ce_type", new("tests.kafka.receive;v=1")),
            new("traceparent", new(Creation)), new("tracestate", new("tenant=original")) }, new WireString("customer-1"));

    private static IHost BuildHost(KafkaFixture fixture, KafkaReceiveControl control, bool missingDeadLetter = false,
        bool healthySibling = false)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Debug).AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss.fff ";
        });
        builder.Services.Configure<HostOptions>(options => options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore);
        builder.Services.AddSingleton(control);
        builder.Services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.UseJsonSerializer(KafkaReceiveJson.Default);
            relayer.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(fixture.Bootstrap);
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest)
                        .WithOffsetCommitMode(OffsetCommitMode.Auto).WithAutoOffsetStore(); // Relayer must override both.
                    options.ConfigureProducer = native => native.WithAllowAutoCreateTopics(false);
                });
                kafka.Consume(fixture.Destination, consumer =>
                {
                    consumer.Options.Group = fixture.Destination;
                    consumer.Options.Failure = new()
                    {
                        RetryDelays = new[] { TimeSpan.Zero }, Jitter = 0,
                        DeadLetterEndpoint = missingDeadLetter ? fixture.Destination + ".missing" : null,
                        CopyTimeout = TimeSpan.FromSeconds(1),
                    };
                    consumer.Handler<KafkaReceiveHandler>();
                });
                if (healthySibling)
                    kafka.Consume(fixture.Destination, consumer =>
                    {
                        consumer.Options.Group = fixture.Destination + ".sibling";
                        consumer.Handler<KafkaReceiveHandler>();
                    });
            });
        });
        return builder.Build();
    }
}

[Message("tests.kafka.receive")]
public sealed record KafkaReceiveMessage(int Number);
public sealed class KafkaReceiveControl
{
    internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal Channel<int> Handled { get; } = Channel.CreateUnbounded<int>();
    internal int FirstAttempts;
    internal KafkaConsumeFeature LastFeature;
}
public sealed class KafkaReceiveHandler(KafkaReceiveControl control) : IHandler<KafkaReceiveMessage>
{
    public async ValueTask HandleAsync(KafkaReceiveMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        control.LastFeature = context.Kafka();
        if (message.Number == 1 && Interlocked.Increment(ref control.FirstAttempts) == 1)
        {
            control.Entered.SetResult();
            await control.Release.Task.WaitAsync(cancellationToken);
            throw new InvalidOperationException("retry me");
        }
        if (message.Number == 2) throw new InvalidOperationException("poison record");
        control.Handled.Writer.TryWrite(message.Number);
    }
}
[JsonSerializable(typeof(KafkaReceiveMessage))]
internal partial class KafkaReceiveJson : JsonSerializerContext;
