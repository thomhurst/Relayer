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
    [Test]
    public async Task Processing_cancellation_is_not_mistaken_for_a_control_wakeup()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.RegisterConsumer(async (_, _) =>
        {
            entered.SetResult();
            await release.Task.WaitAsync(token);
            throw new OperationCanceledException("handler canceled independently");
        });
        await fixture.Transport.StartAsync(token);
        await fixture.Transport.SendAsync(Message(fixture.Destination, 0), token);
        await entered.Task.WaitAsync(token);
        var consumer = ((KafkaProducerTransport)fixture.Transport).Consumers[0];
        var pending = consumer.SeekAsync(new(fixture.Destination, 0, 0), null, token).AsTask();
        release.SetResult();
        await Assert.That(async () => await consumer.Completion.WaitAsync(token)).Throws<OperationCanceledException>();
        await Assert.That(async () => await pending).Throws<InvalidOperationException>().WithMessageContaining("stopped before applying");
    }

    [Test]
    public async Task Offset_replay_drains_inflight_work_and_canceled_requests_do_not_seek()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
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

        using (var canceled = CancellationTokenSource.CreateLinkedTokenSource(token))
        {
            var abandoned = control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 0), canceled.Token).AsTask();
            await Assert.That(abandoned.IsCompleted).IsFalse();
            canceled.Cancel();
            await Assert.That(async () => await abandoned).Throws<OperationCanceledException>();
        }
        var replay = control.SeekAsync(fixture.Destination, new TopicPartitionOffset(fixture.Destination, 0, 1), token).AsTask();
        await Assert.That(replay.IsCompleted).IsFalse();
        state.Release.SetResult();
        await replay;
        foreach (var expected in new long[] { 0, 1, 2, 1, 2 })
            await Assert.That(await state.Handled.Reader.ReadAsync(token)).IsEqualTo(expected);
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
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
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
    public async Task Duplicate_declarations_are_rejected_and_shutdown_cancels_queued_seek()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var state = new KafkaReplayState { BlockFirst = true };
        using var host = BuildHost(fixture, state, TimeSpan.FromMilliseconds(100));
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
        try
        {
            var stopping = host.StopAsync(token);
            await Assert.That(async () => await replay).Throws<OperationCanceledException>();
            await stopping.WaitAsync(TimeSpan.FromSeconds(5), token);
            await Assert.That(state.Release.Task.IsCompleted).IsFalse();
        }
        finally { state.Release.TrySetResult(); }
    }

    private static OutgoingMessage Message(string topic, int number) => new(topic, Encoding.UTF8.GetBytes($"{{\"Number\":{number}}}"),
        new KeyValuePair<string, WireString>[] { new("ce_id", new("record-" + number)), new("ce_type", new("tests.kafka.receive;v=1")) });

    private static IHost BuildHost(KafkaFixture fixture, KafkaReplayState state, TimeSpan? drainTimeout = null)
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
                    if (drainTimeout is { } timeout) consumer.Options.DrainTimeout = timeout;
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
