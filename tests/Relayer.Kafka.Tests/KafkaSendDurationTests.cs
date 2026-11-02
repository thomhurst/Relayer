using Dekaf;
using Dekaf.Serialization;
using Microsoft.Extensions.Options;
using Relayer.Tests;

namespace Relayer.Kafka.Tests;

public class KafkaSendDurationTests
{
    [Test]
    public async Task Acknowledged_singles_batches_mixed_destinations_and_tombstones_record_native_durations()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        await using var fixture = (KafkaFixture)await KafkaFixture.CreateAsync(tracing, token);
        await fixture.Transport.StartAsync(token);
        var first = new OutgoingMessage(fixture.Destination, new byte[] { 1 });
        var other = new OutgoingMessage(fixture.Destination + ".dead-letter", new byte[] { 2 });
        await fixture.Transport.SendAsync(first, token);
        await fixture.Transport.SendBatchAsync(new[] { first, first }, token);
        await fixture.Transport.SendBatchAsync(new[] { first, other }, token);
        await fixture.Transport.SendBatchAsync(new[] { new OutgoingMessage(fixture.Destination, default,
            orderingKey: new WireString("deleted"), isTombstone: true), first }, token);
        await fixture.Transport.SendBatchAsync(Array.Empty<OutgoingMessage>(), token);
        await Assert.That(capture.Values.Count).IsEqualTo(6);
        await Assert.That(capture.Values.Count(value => Equals(value.Tags["messaging.destination.name"], fixture.Destination))).IsEqualTo(5);
        await Assert.That(capture.Values.All(value => value.Seconds > 0 &&
            Equals(value.Tags["messaging.system"], "kafka") && Equals(value.Tags["messaging.operation.name"], "send") &&
            !value.Tags.ContainsKey("error.type"))).IsTrue();
    }

    [Test]
    [Arguments("single")]
    [Arguments("batch")]
    [Arguments("mixed")]
    [Arguments("tombstone")]
    [Arguments("tombstone-batch")]
    public async Task Native_single_and_batch_failures_record_bounded_error(string mode)
    {
        using var instrumentation = new RelayerInstrumentation();
        using var capture = new SendDurationCapture(instrumentation);
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        var producer = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers("localhost:1").WithKeySerializer(new KafkaKeySerializer()).WithValueSerializer(new KafkaValueSerializer()).Build();
        await using var transport = new KafkaProducerTransport("test", producer, ["orders", "other"], tracing);
        await producer.DisposeAsync();
        var message = new OutgoingMessage("orders", new byte[] { 1 }, orderingKey: new WireString("key"), isTombstone: mode.StartsWith("tombstone", StringComparison.Ordinal));
        await Assert.That(async () =>
        {
            if (mode == "mixed") await transport.SendBatchAsync(new[] { message, new OutgoingMessage("other", default) }, default);
            else if (mode is "batch" or "tombstone-batch") await transport.SendBatchAsync(new[] { message, message }, default);
            else await transport.SendAsync(message, default);
        }).Throws<ObjectDisposedException>();
        var duration = capture.Values.Single();
        await Assert.That(duration.Seconds > 0).IsTrue();
        await Assert.That(duration.Tags["messaging.system"]).IsEqualTo("kafka");
        await Assert.That(duration.Tags["messaging.operation.name"]).IsEqualTo("send");
        await Assert.That(duration.Tags["messaging.destination.name"]).IsEqualTo("orders");
        await Assert.That(duration.Tags["error.type"]).IsEqualTo("_OTHER");
    }
}
