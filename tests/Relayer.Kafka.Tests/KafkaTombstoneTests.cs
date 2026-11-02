using System.Buffers;
using System.Text;
using System.Threading.Channels;
using Dekaf;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaTombstoneTests
{
    [Test]
    public async Task Native_null_dispatches_without_deserialization_and_empty_values_ignore_private_headers()
    {
        var services = new ServiceCollection();
        var control = new TombstoneControl();
        services.AddSingleton(control);
        services.AddRelayer(r => r.AddMessageTypesFromThisAssembly().Handler<TombstoneHandler>());
        await using var provider = services.BuildServiceProvider();
        var serializer = new CountingSerializer(provider.GetRequiredService<IMessageSerializer>());
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), serializer, static (_, error, _) => ValueTask.FromException(error));
        foreach (var descriptor in provider.GetRequiredService<HandlerRegistry>().Descriptors) builder.Handler(descriptor);
        var receive = new ReceivePipeline<KafkaDelivery, KafkaMessageBinding>(builder.Build(), new());
        await receive.ProcessAsync(Record(null));
        await Assert.That(serializer.Reads).IsEqualTo(0);
        await Assert.That(await control.Handled.Reader.ReadAsync()).IsEqualTo("deleted:customer-1");
        await Assert.That(async () => await receive.ProcessAsync(Record([])))
            .Throws<MessageDeserializationException>();
        await Assert.That(serializer.Reads).IsEqualTo(1);
        await receive.ProcessAsync(Record("{\"Id\":42}"u8.ToArray()));
        await Assert.That(await control.Handled.Reader.ReadAsync()).IsEqualTo("value:42");
        await Assert.That(serializer.Reads).IsEqualTo(2);
        await Assert.That(async () => await receive.ProcessAsync(Record(null, nullKey: true)))
            .Throws<MessageEnvelopeException>().WithMessageContaining("non-null native key");
    }

    [Test]
    public async Task Native_null_without_tombstone_handler_fails_without_json_deserialization()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new TombstoneControl());
        services.AddRelayer(r => r.AddMessageTypesFromThisAssembly().Handler<TombstoneValueHandler>());
        await using var provider = services.BuildServiceProvider();
        var serializer = new CountingSerializer(provider.GetRequiredService<IMessageSerializer>());
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), serializer, static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<TombstoneOrder, TombstoneValueHandler>()).Build();
        var receive = new ReceivePipeline<KafkaDelivery, KafkaMessageBinding>(pipeline, new());
        await Assert.That(async () => await receive.ProcessAsync(Record(null))).Throws<MessageDeserializationException>();
        await Assert.That(serializer.Reads).IsEqualTo(0);
    }

    [Test]
    public async Task Native_markers_cannot_turn_empty_bodies_into_null_and_outbox_rejects_null_state()
    {
        var state = new KafkaSendState();
        var options = new SendOptions().ConfigureNative<Headers>(static h => h.Add(KafkaSendState.TombstoneHeader, "true"));
        state.Prepare(new("orders", default, options: options));
        await Assert.That(state.Tombstone).IsFalse();
        await Assert.That(state.Headers.GetFirst(KafkaSendState.TombstoneHeader)).IsNull();
        var message = new OutgoingMessage("orders", default, isTombstone: true);
        state.Prepare(message);
        await Assert.That(state.Tombstone).IsTrue();
        await Assert.That(() => message.ValidateForOutbox()).Throws<InvalidOperationException>().WithMessageContaining("tombstones");
        state.Prepare(new("orders", default));
        await Assert.That(state.Tombstone).IsFalse();
    }

    [Test]
    public async Task Typed_send_and_receive_preserve_wire_identity_keys_null_values_and_durable_commit()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        var control = new TombstoneControl { Pause = true };
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Logging.ClearProviders();
        hostBuilder.Services.AddSingleton(control);
        hostBuilder.Services.AddRelayer(r =>
        {
            r.AddMessageTypesFromThisAssembly();
            r.Kafka("events", kafka =>
            {
                kafka.TopologyMode = TopologyMode.None;
                kafka.Options.Configure(o =>
                {
                    o.BootstrapServers.Add(fixture.Bootstrap);
                    o.ConfigureConsumer = native => native.WithAutoOffsetReset(AutoOffsetReset.Earliest);
                });
                kafka.Send<TombstoneOrder>(fixture.Destination);
                kafka.Consume(fixture.Destination, consumer =>
                {
                    consumer.Options.Group = fixture.Destination;
                    consumer.Handler<TombstoneHandler>();
                });
            });
        });
        using var host = hostBuilder.Build();
        await host.StartAsync(token);
        using var scope = host.Services.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        await sender.SendAsync(new TombstoneOrder(42), token);
        await sender.SendAsync(Tombstone.For<TombstoneOrder>("customer-1"), token);
        await control.Entered.Task.WaitAsync(token);
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        reader.Assign(new TopicPartition(fixture.Destination, 0));
        await reader.InitializeAsync(token);
        var committed = await reader.GetCommittedOffsetAsync(new(fixture.Destination, 0), token);
        await Assert.That(committed is null or < 2).IsTrue();
        control.Release.SetResult();
        await Assert.That(await control.Handled.Reader.ReadAsync(token)).IsEqualTo("value:42");
        await Assert.That(await control.Handled.Reader.ReadAsync(token)).IsEqualTo("deleted:customer-1");
        // Stop drains the native commit before inspecting the durable offset.
        await host.StopAsync(token);
        await Assert.That(await reader.GetCommittedOffsetAsync(new(fixture.Destination, 0), token)).IsEqualTo(2L);
        reader.Seek(new TopicPartitionOffset(fixture.Destination, 0, 1));
        var record = (await reader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(record.Value).IsNull();
        await Assert.That(Encoding.UTF8.GetString(record.Key!)).IsEqualTo("customer-1");
        await Assert.That(record.Headers.First(h => h.Key == "ce_type").GetValueAsString()).IsEqualTo("tests.kafka.tombstone;v=3");
        await Assert.That(record.Headers.Any(h => h.Key == KafkaSendState.TombstoneHeader)).IsFalse();
    }

    [Test]
    public async Task Mixed_native_batch_preserves_null_and_empty_values_with_headers()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await fixture.Transport.StartAsync(token);
        KeyValuePair<string, WireString>[] headers = [new("ce_type", new("tests.kafka.tombstone;v=3")), new("ce_id", new("batch"))];
        OutgoingMessage[] messages = [new(fixture.Destination, default, headers, new WireString("delete"), isTombstone: true),
            new(fixture.Destination, default, headers, new WireString("empty"))];
        await fixture.Transport.SendBatchAsync(messages, token);
        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(fixture.Bootstrap)
            .WithGroupId(fixture.Destination).WithAutoOffsetReset(AutoOffsetReset.Earliest).Build();
        reader.Assign(new TopicPartition(fixture.Destination, 0));
        await reader.InitializeAsync(token);
        var deleted = (await reader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        var empty = (await reader.ConsumeOneAsync(TimeSpan.FromSeconds(30), token))!.Value;
        await Assert.That(deleted.Value).IsNull();
        await Assert.That(empty.Value is { Length: 0 }).IsTrue();
        await Assert.That(deleted.Headers.First(h => h.Key == "ce_type").GetValueAsString()).IsEqualTo("tests.kafka.tombstone;v=3");
        await Assert.That(empty.Headers.First(h => h.Key == "ce_id").GetValueAsString()).IsEqualTo("batch");
    }

    private static KafkaDelivery Record(byte[]? body, bool nullKey = false) => new(new ConsumeResult<byte[], byte[]>(
        "orders", 0, 0, "customer-1"u8.ToArray(), nullKey, body!, body is null,
        new Headers().Add("ce_type", "tests.kafka.tombstone;v=3").Add("ce_id", "record")
            .Add(KafkaSendState.TombstoneHeader, "true"), 0, TimestampType.CreateTime, null, Serializers.ByteArray, Serializers.ByteArray), "0");

    private sealed class CountingSerializer(IMessageSerializer inner) : IMessageSerializer
    {
        internal int Reads;
        public string ContentType => inner.ContentType;
        public void Serialize<T>(T message, IBufferWriter<byte> destination) => inner.Serialize(message, destination);
        public T Deserialize<T>(ReadOnlySequence<byte> body) { Reads++; return inner.Deserialize<T>(body); }
    }
}

[Message("tests.kafka.tombstone", Version = 3)]
public sealed record TombstoneOrder(int Id);
public sealed class TombstoneControl
{
    internal Channel<string> Handled { get; } = Channel.CreateUnbounded<string>();
    internal bool Pause;
    internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
public sealed class TombstoneHandler(TombstoneControl control) : IHandler<TombstoneOrder>, IHandler<Tombstone<TombstoneOrder>>
{
    public ValueTask HandleAsync(TombstoneOrder message, MessageContext context, CancellationToken token)
    {
        control.Handled.Writer.TryWrite("value:" + message.Id);
        return default;
    }
    public async ValueTask HandleAsync(Tombstone<TombstoneOrder> message, MessageContext context, CancellationToken token)
    {
        if (control.Pause)
        {
            control.Entered.TrySetResult();
            await control.Release.Task.WaitAsync(token);
        }
        control.Handled.Writer.TryWrite("deleted:" + message.Key);
    }
}
public sealed class TombstoneValueHandler : IHandler<TombstoneOrder>
{
    public ValueTask HandleAsync(TombstoneOrder message, MessageContext context, CancellationToken token) => default;
}
