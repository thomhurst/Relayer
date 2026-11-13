using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json.Serialization;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Relayer.Transports;
using Testcontainers.Kafka;

namespace Relayer.Kafka.Tests;

public class KafkaBrokerTests
{
    [Test]
    public async Task Named_sender_and_ordered_batches_round_trip_with_native_tracing_and_reusable_state()
    {
        var brokerBuilder = new KafkaBuilder("apache/kafka:4.0.2").WithVendor(KafkaVendor.ApacheSoftwareFoundation);
        if (OperatingSystem.IsLinux())
        {
            // Avoid Linux's default outbound ephemeral range (32768-60999). Docker can exhaust
            // its ten bind attempts on occupied ephemeral ports: moby/moby#54079.
            brokerBuilder = brokerBuilder.WithPortBinding("20000-29999", "9092").WithPortBinding("20000-29999", "9093");
        }
        await using var broker = brokerBuilder.AfterStartupCopy().Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var token = timeout.Token;
        await broker.StartAsync(token);
        // Inspect the actual JVM, not just builder options: startup wrappers must preserve
        // the same resource limits as the restartable fixtures running beside this test.
        var launch = await broker.ExecAsync(["cat", "/proc/1/cmdline"], token);
        await Assert.That(launch.ExitCode).IsEqualTo(0L);
        await Assert.That(launch.Stdout).Contains("-Xms128m\0");
        await Assert.That(launch.Stdout).Contains("-Xmx256m\0");
        await Assert.That(launch.Stdout).Contains("-XX:ActiveProcessorCount=2\0");
        await Assert.That(launch.Stdout).Contains("-XX:TieredStopAtLevel=1\0");
        var bootstrap = broker.GetBootstrapAddress();
        var topic = "relayer-" + Guid.NewGuid().ToString("N");
        var other = topic + "-other";
        await using var admin = new AdminClientBuilder().WithBootstrapServers(bootstrap).Build();
        await admin.CreateTopicsAsync([new() { Name = topic, NumPartitions = 3, ReplicationFactor = 1 },
            new() { Name = other, NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);

        var spans = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is "Relayer" or "Dekaf",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        var log = new NativeLogProvider();
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Logging.ClearProviders();
        hostBuilder.Logging.AddProvider(log);
        var configured = false;
        hostBuilder.Services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Order>("tests.kafka.order");
            builder.Infrastructure().MessageTypes.Add<OtherOrder>("tests.kafka.other");
            builder.UseJsonSerializer(KafkaJson.Default);
            builder.Kafka("events", kafka =>
            {
                kafka.Options.Configure(options =>
                {
                    options.BootstrapServers.Add(bootstrap);
                    options.ConfigureProducer = producer =>
                    {
                        configured = true;
                        producer.WithClientId("relayer-test").WithLinger(TimeSpan.Zero).WithAllowAutoCreateTopics(false);
                    };
                });
                kafka.Send<Order>(topic, route => route.OrderingKey(static order => order.Customer));
                kafka.Send<OtherOrder>(other);
            });
        });
        using var host = hostBuilder.Build();
        await host.StartAsync(token);
        var transport = host.Services.GetServices<ITransport>().Single();
        await Assert.That(configured).IsTrue();
        await Assert.That(transport.Name).IsEqualTo("events");
        await Assert.That(log.Categories.Any(category => category.StartsWith("Dekaf", StringComparison.Ordinal))).IsTrue();
        await Assert.That(transport.GetCapabilities(topic).MaxBatchSize).IsEqualTo(int.MaxValue);
        await Assert.That(transport.GetCapabilities(topic).DeadLetter).IsFalse();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        await Assert.That(async () => await transport.SendAsync(new("unknown", default), token))
            .Throws<InvalidOperationException>().WithMessageContaining("undeclared topic");
        await Assert.That(async () => await transport.SendAsync(new(topic, default, deliverAt: DateTimeOffset.UtcNow), token))
            .Throws<NotSupportedException>();
        using (var cancelled = new CancellationTokenSource())
        {
            cancelled.Cancel();
            await Assert.That(async () => await transport.SendAsync(new(topic, default), cancelled.Token))
                .Throws<OperationCanceledException>();
            await Assert.That(async () => await transport.SendBatchAsync(ReadOnlyMemory<OutgoingMessage>.Empty, cancelled.Token))
                .Throws<OperationCanceledException>();
        }
        var failing = new SendOptions().ConfigureNative<Headers>(static _ => throw new InvalidOperationException("callback failure"));
        await Assert.That(async () => await transport.SendAsync(new(topic, default, options: failing), token))
            .Throws<InvalidOperationException>().WithMessageContaining("callback failure");
        await Assert.That(async () => await transport.SendBatchAsync(new OutgoingMessage[]
        {
            new(topic, default), new(topic, default, options: failing),
        }, token)).Throws<InvalidOperationException>();

        using var scope = host.Services.CreateScope();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        const string creation = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        await sender.SendAsync(new Order("customer-1", 0), options =>
        {
            options.ConfigureNative<Headers>(static headers => headers.Add("native", "configured"));
        }, token);
        var senderCreation = spans.Last(span => span.Source.Name == "Relayer" && span.DisplayName == "send " + topic).Id;
        KeyValuePair<string, WireString>[] originalHeaders = [new("traceparent", new(creation)), new("tracestate", new("tenant=original"))];
        var batch = new OutgoingMessage[32];
        for (var i = 0; i < batch.Length; i++)
            batch[i] = new(topic, Encoding.UTF8.GetBytes(i.ToString()), originalHeaders, orderingKey: new WireString("customer-1"));
        await transport.SendBatchAsync(batch, token);
        await transport.SendAsync(new(topic, "no-key"u8.ToArray()), token);
        await transport.SendAsync(new(topic, "empty-key"u8.ToArray(), orderingKey: new WireString("")), token);
        await transport.SendBatchAsync(new OutgoingMessage[] { new(other, "other"u8.ToArray()), new(topic, "mixed"u8.ToArray()) }, token);
        await transport.SendBatchAsync(ReadOnlyMemory<OutgoingMessage>.Empty, token);

        await using var consumer = new ConsumerBuilder<byte[], byte[]>()
            .WithBootstrapServers(bootstrap).WithGroupId("read-" + Guid.NewGuid().ToString("N"))
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithAutoCommitInterval(TimeSpan.FromHours(1))
            .Build();
        consumer.Assign(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2));
        await consumer.InitializeAsync(token);
        var received = 0;
        var partition = -1;
        var expected = 0;
        var noKey = false;
        var emptyKey = false;
        var mixed = false;
        while (received < batch.Length + 4)
        {
            var result = await consumer.ConsumeOneAsync(TimeSpan.FromSeconds(30), token);
            if (result is not { } message) throw new InvalidOperationException("Kafka receive timed out.");
            var value = Encoding.UTF8.GetString(message.Value);
            if (value.StartsWith('{'))
            {
                partition = message.Partition;
                await Assert.That(Encoding.UTF8.GetString(message.Key!)).IsEqualTo("customer-1");
                await Assert.That(ReadHeader(message.Headers, "ce_type")).IsEqualTo("tests.kafka.order;v=1");
                await Assert.That(ReadHeader(message.Headers, "native")).IsEqualTo("configured");
                await Assert.That(ReadHeader(message.Headers, "traceparent")).IsEqualTo(senderCreation);
                await Assert.That(message.Headers.Count(header => header.Key == "traceparent")).IsEqualTo(1);
            }
            else if (value == "no-key") { noKey = message.Key is null; }
            else if (value == "empty-key") { emptyKey = message.Key is { Length: 0 }; }
            else if (value == "mixed") { mixed = true; }
            else
            {
                await Assert.That(message.Partition).IsEqualTo(partition);
                await Assert.That(int.Parse(value)).IsEqualTo(expected++);
                await Assert.That(ReadHeader(message.Headers, "traceparent")).IsEqualTo(creation);
                await Assert.That(ReadHeader(message.Headers, "tracestate")).IsEqualTo("tenant=original");
                await Assert.That(message.Headers!.Count(header => header.Key == "traceparent")).IsEqualTo(1);
                await Assert.That(ReadHeader(message.Headers, "native")).IsNull();
            }
            received++;
        }
        await Assert.That(expected).IsEqualTo(32);
        await Assert.That(noKey && emptyKey && mixed).IsTrue();
        var sendSpans = spans.Where(span => span.Source.Name == "Relayer" && span.DisplayName == "send " + topic).ToArray();
        await Assert.That(sendSpans.Length).IsGreaterThan(0);
        await Assert.That(sendSpans.All(span => Equals(span.GetTagItem("messaging.system"), "kafka"))).IsTrue();
        await Assert.That(spans.Any(span => span.Source.Name == "Dekaf" && sendSpans.Any(parent => parent.SpanId == span.ParentSpanId))).IsTrue();
        await host.StopAsync(token);
    }

    private static string? ReadHeader(IReadOnlyList<Header>? headers, string name) =>
        headers?.FirstOrDefault(header => header.Key == name) is { Key: not null } header ? header.GetValueAsString() : null;

    private sealed class NativeLogProvider : ILoggerProvider
    {
        internal ConcurrentBag<string> Categories { get; } = [];
        public ILogger CreateLogger(string categoryName)
        {
            Categories.Add(categoryName);
            return Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        }
        public void Dispose() { }
    }
}

[Message("tests.kafka.order")]
public sealed record Order(string Customer, int Number);
[Message("tests.kafka.other")]
public sealed record OtherOrder(int Number);
[JsonSerializable(typeof(Order))]
[JsonSerializable(typeof(OtherOrder))]
internal partial class KafkaJson : JsonSerializerContext;
