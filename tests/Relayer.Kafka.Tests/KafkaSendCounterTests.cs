using System.Diagnostics.Metrics;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Serialization;
using Microsoft.Extensions.Options;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

public class KafkaSendCounterTests
{
    [Test]
    public async Task Native_send_batch_tombstone_and_preparation_boundaries_count_once()
    {
        await using var broker = new KafkaBuilder("apache/kafka:4.0.2").WithVendor(KafkaVendor.ApacheSoftwareFoundation).AfterStartupCopy().Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.StartAsync(token);
        var topic = "counter-" + Guid.NewGuid().ToString("N");
        await using var admin = new AdminClientBuilder().WithBootstrapServers(broker.GetBootstrapAddress()).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
        using var instrumentation = new RelayerInstrumentation();
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), [], instrumentation: instrumentation);
        long sent = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, metrics) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, instrumentation) && instrument.Name == "messaging.client.sent.messages")
                    metrics.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, count, _, _) => Interlocked.Add(ref sent, count));
        listener.Start();
        var producer = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers(broker.GetBootstrapAddress()).WithKeySerializer(new KafkaKeySerializer())
            .WithValueSerializer(new KafkaValueSerializer()).Build();
        await using var transport = new KafkaProducerTransport("kafka", producer, [topic], tracing);
        await transport.StartAsync(token);
        var message = new OutgoingMessage(topic, new byte[] { 1 });
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.That(async () => await transport.SendAsync(message, cancelled.Token)).Throws<OperationCanceledException>();
        var options = new SendOptions().ConfigureNative<Headers>(static _ => throw new InvalidOperationException("prepare failed"));
        var invalid = new OutgoingMessage(topic, default, options: options);
        await Assert.That(async () => await transport.SendAsync(invalid, token)).Throws<InvalidOperationException>();
        await Assert.That(async () => await transport.SendBatchAsync(new[] { message, invalid, message }, token)).Throws<InvalidOperationException>();
        await Assert.That(sent).IsEqualTo(0);
        await transport.SendAsync(message, token);
        await Assert.That(sent).IsEqualTo(1);
        await transport.SendBatchAsync(new[] { message, message }, token);
        await Assert.That(sent).IsEqualTo(3);
        var tombstone = new OutgoingMessage(topic, default, orderingKey: new WireString("key"), isTombstone: true);
        await transport.SendBatchAsync(new[] { tombstone, message, tombstone }, token);
        await Assert.That(sent).IsEqualTo(6);
    }
}
