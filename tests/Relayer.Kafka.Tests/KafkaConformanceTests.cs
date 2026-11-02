using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Microsoft.Extensions.Logging;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaConformanceTests
{
    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.Reconnect)]
    public async Task Kafka_passes(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<KafkaDelivery, KafkaMessageBinding>(KafkaFixture.CreateAsync, TimeSpan.FromMinutes(2));
        await Assert.That((await suite.RunAsync(scenario)).Skipped).IsFalse();
    }
}

internal sealed class KafkaFixture : ITransportConformanceFixture<KafkaDelivery>
{
    internal KafkaRestartableBroker Broker { get; }
    private readonly KafkaProducerTransport _transport;
    private readonly MessageTracing _tracing;
    private readonly ILoggerFactory _logs = LoggerFactory.Create(builder => builder
        .SetMinimumLevel(LogLevel.Debug).AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss.fff ";
        }));
    internal string Bootstrap => Broker.Bootstrap;
    public string Destination { get; } = "relayer-" + Guid.NewGuid().ToString("N");
    public ITransport Transport => _transport;
    public string MessagingSystem => "kafka";
    public string SettlementOperation => "commit";
    public HeaderNames HeaderNames { get; } = new();

    private KafkaFixture(KafkaRestartableBroker broker, MessageTracing tracing)
    {
        Broker = broker;
        _tracing = tracing;
        var producer = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers(Bootstrap).WithLoggerFactory(_logs)
            .WithKeySerializer(new KafkaKeySerializer()).WithValueSerializer(new KafkaValueSerializer()).Build();
        _transport = new("conformance", producer, [Destination, Destination + ".dead-letter"], tracing);
    }

    internal static async ValueTask<ITransportConformanceFixture<KafkaDelivery>> CreateAsync(MessageTracing tracing, CancellationToken token)
    {
        var broker = new KafkaRestartableBroker();
        KafkaFixture? fixture = null;
        try
        {
            await broker.StartAsync(token);
            fixture = new KafkaFixture(broker, tracing);
            await using var admin = new AdminClientBuilder().WithBootstrapServers(fixture.Bootstrap).WithLoggerFactory(fixture._logs).Build();
            await admin.CreateTopicsAsync([new() { Name = fixture.Destination, NumPartitions = 1, ReplicationFactor = 1 },
                new() { Name = fixture.Destination + ".dead-letter", NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
            return fixture;
        }
        catch
        {
            if (fixture is null) await broker.DisposeAsync();
            else await fixture.DisposeAsync();
            throw;
        }
    }

    public void RegisterConsumer(Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        var ownership = new KafkaOwnership();
        var consumer = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(Bootstrap).WithGroupId(Destination)
            .WithLoggerFactory(_logs)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithOffsetCommitMode(OffsetCommitMode.Manual)
            .WithAutoOffsetStore(false).AddRebalanceListener(ownership).SubscribeTo(Destination).Build();
        _transport.Consumers.Add(new(consumer, receive, _tracing,
            new() { System = "kafka", Destination = Destination, ConsumerGroup = Destination }, ownership));
    }

    // Completion is acknowledged once per native fetch batch by KafkaConsumer, outside processing spans.
    public ValueTask SettleAsync(KafkaDelivery incoming, Disposition disposition, CancellationToken cancellationToken)
    {
        if (disposition.Kind != DispositionKind.Complete) throw new NotSupportedException("Kafka has no native retry or dead-letter settlement.");
        return default;
    }
    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task DisconnectAsync(CancellationToken cancellationToken) => Broker.StopAsync(cancellationToken);
    public async Task ReconnectAsync(CancellationToken cancellationToken)
    {
        await Broker.RestartAsync(cancellationToken);
        // Process startup can finish before Kafka accepts protocol requests after a restart.
        // Synchronize with live native discovery, without changing the conformance deadline.
        await using var admin = new AdminClientBuilder().WithBootstrapServers(Bootstrap).WithLoggerFactory(_logs).Build();
        await admin.DescribeClusterAsync(new DescribeClusterOptions(), cancellationToken);
    }
    public async ValueTask DisposeAsync()
    {
        try { await _transport.DisposeAsync(); }
        finally
        {
            try { await Broker.DisposeAsync(); }
            finally { _logs.Dispose(); }
        }
    }
}
