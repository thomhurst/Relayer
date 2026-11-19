using System.Text;
using System.Threading.Channels;
using Dekaf;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaSeekTests
{
    [ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
    public required KafkaHeaderSnapshotBroker Broker { get; init; }

    private static string Topic() => TestContext.Current!.Isolation.GetIsolatedName("seek").ToLowerInvariant();

    [Test]
    public async Task Handler_cancellation_during_a_seek_leaves_the_record_for_replay()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await Broker.Started.WaitAsync(token);
        await using var fixture = await KafkaFixture.CreateAsync(tracing, Broker.Bootstrap, Topic(), token);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var replayed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        fixture.RegisterConsumer(async (_, cancellationToken) =>
        {
            if (Interlocked.Increment(ref attempts) > 1)
            {
                replayed.TrySetResult();
                return Disposition.Complete;
            }
            entered.SetResult();
            // Under the Cancel stop policy Dekaf marks the lane as stopping before it cancels this token,
            // so the handler's own exception below is always thrown while the seek stops the lane.
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken).WaitAsync(token);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            throw new OperationCanceledException("handler canceled independently");
        }, new()
        {
            Ordering = PartitionedProcessingOrder.Key, MaxConcurrentHandlersPerPartition = 1,
            CommitPolicy = PartitionCommitPolicy.CommitCompletedPeriodically, StopPolicy = PartitionStopPolicy.Cancel,
        });
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 0), token);
        await entered.Task.WaitAsync(token);
        var consumer = ((KafkaProducerTransport)fixture.Transport).Consumers[0];
        var pending = consumer.SeekAsync(new(fixture.Destination, 0, 0), null, token).AsTask();
        // Dekaf treats an OperationCanceledException thrown once the lane is stopping as leaving the record
        // unprocessed: the seek applies, the record replays from the seek offset and the consumer keeps running.
        await pending.WaitAsync(token);
        await replayed.Task.WaitAsync(token);
        await Assert.That(consumer.Completion.IsCompleted).IsFalse();
    }

    [Test]
    public async Task Offset_replay_waits_for_inflight_work_and_canceled_requests_do_not_seek()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await Broker.Started.WaitAsync(token);
        await using var fixture = await KafkaFixture.CreateAsync(tracing, Broker.Bootstrap, Topic(), token);
        var state = new KafkaReplayState { BlockFirst = true };
        using var host = BuildHost(fixture, state);
        var control = host.Services.GetRequiredKeyedService<IKafkaConsumerControl>("events");
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 0), token))
            .Throws<InvalidOperationException>().WithMessageContaining("Start the host");
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendBatchAsync(new OutgoingMessage[]
        {
            Message(fixture.Destination, 0), Message(fixture.Destination, 1), Message(fixture.Destination, 2),
        }, token);
        await host.StartAsync(token);
        await state.Entered.Task.WaitAsync(token);

        // A request canceled before the runtime starts it never seeks.
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 0),
            new CancellationToken(canceled: true))).Throws<OperationCanceledException>();
        var replay = control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 1), token).AsTask();
        await Assert.That(replay.IsCompleted).IsFalse();
        state.Release.SetResult();
        await replay;
        // Dekaf applies the seek on its runtime loop, so records queued behind the held one can still
        // run before the seek discards the rest (#798). A sentinel after the seek marks the end.
        await fixture.Transport.SendAsync(Message(fixture.Destination, 3), token);
        var handled = new List<long>();
        while (handled.Count == 0 || handled[^1] != 3)
            handled.Add(await state.Handled.Reader.ReadAsync(token));
        // Offset 0 finishes first, any records handled before the seek follow in order, then replay restarts at offset 1.
        long[] expected = [0, .. Enumerable.Range(1, Math.Max(0, handled.Count - 4)).Select(static i => (long)i), 1, 2, 3];
        await Assert.That(string.Join(",", handled)).IsEqualTo(string.Join(",", expected));
        await host.StopAsync(token);
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 0), token))
            .Throws<InvalidOperationException>().WithMessageContaining("not running");
    }

    [Test]
    public async Task Timestamp_replay_uses_native_lookup_and_invalid_targets_leave_consumer_running()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await Broker.Started.WaitAsync(token);
        await using var fixture = await KafkaFixture.CreateAsync(tracing, Broker.Bootstrap, Topic(), token);
        var state = new KafkaReplayState();
        using var host = BuildHost(fixture, state);
        await using var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap).BuildAsync(token);
        // Use recent timestamps so Kafka's retention and timestamp validation accept the records.
        var start = DateTimeOffset.UtcNow.AddMinutes(-1);
        for (var i = 0; i < 3; i++)
        {
            var message = Message(fixture.Destination, i);
            await producer.ProduceAsync(new()
            {
                Topic = fixture.Destination, Partition = 0, Key = "key"u8.ToArray(), Value = message.Body.ToArray(),
                Headers = new Headers().Add("ce_id", "record-" + i).Add("ce_type", "tests.kafka.receive;v=1"),
                Timestamp = start.AddSeconds(i),
            }, token);
        }
        await host.StartAsync(token);
        var control = host.Services.GetRequiredKeyedService<IKafkaConsumerControl>("events");
        for (var i = 0; i < 3; i++)
            await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo((long)i);
        await control.SeekAsync(fixture.Destination, new TopicPartitionTimestamp(new(fixture.Destination, 0), start.AddSeconds(1)), token);
        await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo(1L);
        await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo(2L);

        await Assert.That(async () => await control.SeekAsync(fixture.Destination,
                new TopicPartitionTimestamp(new(fixture.Destination, 0), start.AddDays(1)), token))
            .Throws<InvalidOperationException>().WithMessageContaining("no matching timestamp offset");
        await Assert.That(async () => await control.SeekAsync("wrong-group", new TopicPartitionOffset(fixture.Destination, 0, 0), token))
            .Throws<InvalidOperationException>().WithMessageContaining("no running consumer");
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset("wrong-topic", 0, 0), token))
            .Throws<InvalidOperationException>().WithMessageContaining("no running consumer");
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 1, 0), token))
            .Throws<InvalidOperationException>().WithMessageContaining("not assigned");
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, -1, 0), token))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, -1), token))
            .Throws<ArgumentOutOfRangeException>();
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 3), token);
        await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo(3L);
        await host.StopAsync(token);
    }

    [Test]
    public async Task Duplicate_declarations_are_rejected_and_shutdown_fails_a_pending_seek()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await Broker.Started.WaitAsync(token);
        await using var fixture = await KafkaFixture.CreateAsync(tracing, Broker.Bootstrap, Topic(), token);
        var state = new KafkaReplayState { BlockFirst = true };
        using var host = BuildHost(fixture, state);
        await host.StartAsync(token);
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 0), token);
        await state.Entered.Task.WaitAsync(token);
        var control = host.Services.GetRequiredKeyedService<IKafkaConsumerControl>("events");
        var transport = host.Services.GetRequiredKeyedService<KafkaProducerTransport>("events");
        // The receiver list is complete after startup. Temporarily duplicate the declaration
        // to test ambiguity without introducing a second group member and a rebalance.
        transport.Consumers.Add(transport.Consumers[0]);
        await Assert.That(async () => await control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 0), token))
            .Throws<InvalidOperationException>().WithMessageContaining("multiple consumer declarations");
        transport.Consumers.RemoveAt(1);
        var replay = control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 0), token).AsTask();
        var stopping = host.StopAsync(token);
        state.Release.SetResult();
        await Assert.That(async () => await replay).Throws<InvalidOperationException>().WithMessageContaining("stopped before the seek");
        await stopping;
    }

    private static OutgoingMessage Message(string topic, int number) => new(topic, Encoding.UTF8.GetBytes($"{{\"Number\":{number}}}"),
        new KeyValuePair<string, WireString>[] { new("ce_id", new("record-" + number)), new("ce_type", new("tests.kafka.receive;v=1")) });

    private static IHost BuildHost(KafkaFixture fixture, KafkaReplayState state)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(state);
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
                    options.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                kafka.Consume(fixture.Destination, consumer =>
                {
                    consumer.Options.Group = fixture.Destination;
                    consumer.Handler<KafkaReplayHandler>();
                });
            });
        });
        return builder.Build();
    }
}

public sealed class KafkaReplayState
{
    internal bool BlockFirst { get; init; }
    internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal Channel<long> Handled { get; } = Channel.CreateUnbounded<long>();
}

public sealed class KafkaReplayHandler(KafkaReplayState state) : IHandler<KafkaReceiveMessage>
{
    public async ValueTask HandleAsync(KafkaReceiveMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        if (state.BlockFirst && state.Entered.TrySetResult())
            await state.Release.Task.WaitAsync(cancellationToken);
        state.Handled.Writer.TryWrite(context.Kafka().ConsumeResult.Offset);
    }
}
