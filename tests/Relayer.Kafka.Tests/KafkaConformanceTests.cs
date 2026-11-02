using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Relayer.Testing;
using Relayer.Transports;
using Testcontainers.Kafka;

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
    private readonly KafkaContainer _broker;
    private readonly KafkaProducerTransport _transport;
    private readonly MessageTracing _tracing;
    private readonly ILoggerFactory _logs = LoggerFactory.Create(builder => builder
        .SetMinimumLevel(LogLevel.Debug).AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss.fff ";
        }));
    internal string Bootstrap => _broker.GetBootstrapAddress();
    public string Destination { get; } = "relayer-" + Guid.NewGuid().ToString("N");
    public ITransport Transport => _transport;
    public string MessagingSystem => "kafka";
    public string SettlementOperation => "commit";
    public HeaderNames HeaderNames { get; } = new();

    private KafkaFixture(KafkaContainer broker, MessageTracing tracing)
    {
        _broker = broker;
        _tracing = tracing;
        var producer = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers(Bootstrap).WithLoggerFactory(_logs)
            .WithKeySerializer(new KafkaKeySerializer()).WithValueSerializer(new KafkaValueSerializer()).Build();
        _transport = new("conformance", producer, [Destination, Destination + ".dead-letter"], tracing);
    }

    internal static async ValueTask<ITransportConformanceFixture<KafkaDelivery>> CreateAsync(MessageTracing tracing, CancellationToken token)
    {
        // Docker's random port bindings change across Stop/Start. Pin the advertised client
        // port so reconnect tests restore the same broker address used by existing clients.
        var clientPort = ReserveClientPort();
        var builder = new KafkaBuilder("apache/kafka:4.0.2").WithVendor(KafkaVendor.ApacheSoftwareFoundation)
            .WithPortBinding(clientPort, 9092);
        if (OperatingSystem.IsLinux()) builder = builder.WithPortBinding("20000-29999", "9093");
        var broker = builder.Build();
        KafkaFixture? fixture = null;
        try
        {
            await broker.StartAsync(token);
            fixture = new KafkaFixture(broker, tracing);
            await using var admin = new AdminClientBuilder().WithBootstrapServers(fixture.Bootstrap).Build();
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

    private static int ReserveClientPort()
    {
        var start = OperatingSystem.IsLinux() ? Random.Shared.Next(20000, 30000) : 0;
        for (var attempt = 0; attempt < 10000; attempt++)
        {
            var candidate = start == 0 ? 0 : 20000 + (start - 20000 + attempt) % 10000;
            var listener = new TcpListener(IPAddress.Loopback, candidate);
            try
            {
                listener.Start();
                return ((IPEndPoint)listener.LocalEndpoint).Port;
            }
            catch (SocketException error) when (OperatingSystem.IsLinux() && error.SocketErrorCode == SocketError.AddressAlreadyInUse) { }
            finally { listener.Stop(); }
        }
        throw new InvalidOperationException("No available Kafka test port in 20000-29999.");
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
    public Task DisconnectAsync(CancellationToken cancellationToken) => _broker.StopAsync(cancellationToken);
    public async Task ReconnectAsync(CancellationToken cancellationToken)
    {
        await _broker.StartAsync(cancellationToken);
        // Container startup can finish before Kafka accepts protocol requests after a restart.
        // Synchronize with live native discovery, without changing the conformance deadline.
        await using var admin = new AdminClientBuilder().WithBootstrapServers(Bootstrap).WithLoggerFactory(_logs).Build();
        await admin.DescribeClusterAsync(new DescribeClusterOptions(), cancellationToken);
    }
    public async ValueTask DisposeAsync()
    {
        try { await _transport.DisposeAsync(); }
        finally
        {
            try { await _broker.DisposeAsync(); }
            finally { _logs.Dispose(); }
        }
    }
}
