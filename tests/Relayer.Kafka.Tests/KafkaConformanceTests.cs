using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Producer;
using Dekaf.Serialization;
using Microsoft.Extensions.Logging;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Kafka.Tests;

public class KafkaConformanceTests
{
    [ClassDataSource<KafkaHeaderSnapshotBroker>(Shared = SharedType.PerTestSession)]
    public required KafkaHeaderSnapshotBroker Broker
    {
        get; init;
    }

    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.CleanShutdown)]
    public async Task Kafka_passes(TransportConformanceCase scenario)
    {
        // The fixture uses the destination as its consumer group ID.
        var destination = TestContext.Current!.Isolation.GetIsolatedName("conformance").ToLowerInvariant();
        var suite = new TransportConformanceSuite<KafkaDelivery, KafkaMessageBinding>(async (tracing, token) =>
        {
            await Broker.Started.WaitAsync(token);
            return await KafkaFixture.CreateAsync(tracing, Broker.Bootstrap, destination, token);
        }, TimeSpan.FromMinutes(2));
        await Assert.That((await suite.RunAsync(scenario)).Skipped).IsFalse();
    }
}

public class KafkaOutageConformanceTests
{
    [Test]
    [Arguments(TransportConformanceCase.Reconnect)]
    [Arguments(TransportConformanceCase.SendDuringOutage)]
    public async Task Kafka_passes(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<KafkaDelivery, KafkaMessageBinding>(KafkaFixture.CreateAsync, TimeSpan.FromMinutes(2));
        await Assert.That((await suite.RunAsync(scenario)).Skipped).IsFalse();
    }
}

internal sealed class KafkaFixture : ITransportConformanceFixture<KafkaDelivery>, ITransportShutdownConformanceFixture
{
    // Null when the fixture runs on a broker that the test session shares.
    private readonly KafkaRestartableBroker? _broker;

    internal KafkaRestartableBroker Broker => _broker ?? throw new InvalidOperationException("This fixture does not own its broker.");
    private readonly KafkaProducerTransport _transport;
    private readonly MessageTracing _tracing;

    public ConformanceLogCapture Logs
    {
        get;
    } = new();
    private readonly NativeLifecycleProbe _producerLifetime = new();
    // Each consumer start builds a new native consumer; this probes the latest one.
    private NativeLifecycleProbe _consumerLifetime = new();
    private bool _verifyShutdown;
    private readonly ILoggerFactory _logs;

    private static ILoggerFactory CreateLogs(ConformanceLogCapture capture)
    {
        return LoggerFactory.Create(builder => builder.AddProvider(capture)
                .SetMinimumLevel(LogLevel.Debug).AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "HH:mm:ss.fff ";
                }));
    }

    internal string Bootstrap
    {
        get;
    }

    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(5);

    public bool IsExpectedOutageFailure(Exception error)
    {
        return error is Dekaf.Errors.KafkaTimeoutException or Dekaf.Errors.KafkaException { IsRetriable: true };
    }

    public string Destination
    {
        get;
    }

    public ITransport Transport => _transport;

    public string MessagingSystem => "kafka";

    public string SettlementOperation => "commit";

    public HeaderNames HeaderNames
    {
        get;
    } = new();

    private KafkaFixture(KafkaRestartableBroker? broker, string bootstrap, string destination, MessageTracing tracing)
    {
        _broker = broker;
        Bootstrap = bootstrap;
        Destination = destination;
        _logs = CreateLogs(Logs);
        _tracing = tracing;
        var producer = new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers(Bootstrap).WithLoggerFactory(_logs)
            .WithKeySerializer(new KafkaKeySerializer()).WithValueSerializer(new KafkaValueSerializer()).Build();
        _transport = new("conformance", NativeLifecycleProxy.Wrap<IKafkaProducer<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>>(producer, _producerLifetime),
            [Destination, Destination + ".dead-letter"], tracing, _logs);
    }

    internal static async ValueTask<ITransportConformanceFixture<KafkaDelivery>> CreateAsync(MessageTracing tracing, CancellationToken token)
    {
        var broker = new KafkaRestartableBroker();
        KafkaFixture? fixture = null;

        try
        {
            await broker.StartAsync(token);
            fixture = new KafkaFixture(broker, broker.Bootstrap, "relayer-" + Guid.NewGuid().ToString("N"), tracing);
            await fixture.CreateTopicsAsync(token);
            return fixture;
        }
        catch
        {
            if (fixture is null)
            {
                await broker.DisposeAsync();
            }
            else
            {
                await fixture.DisposeAsync();
            }

            throw;
        }
    }

    /// <summary>Creates a fixture on a running broker it does not own, for tests that leave the broker running.</summary>
    internal static async ValueTask<KafkaFixture> CreateAsync(MessageTracing tracing, string bootstrap, string destination, CancellationToken token)
    {
        var fixture = new KafkaFixture(null, bootstrap, destination, tracing);

        try
        {
            await fixture.CreateTopicsAsync(token);
            return fixture;
        }
        catch
        {
            await fixture.DisposeAsync();
            throw;
        }
    }

    private async Task CreateTopicsAsync(CancellationToken token)
    {
        await using var admin = new AdminClientBuilder().WithBootstrapServers(Bootstrap).WithLoggerFactory(_logs).Build();
        await admin.CreateTopicsAsync([new() { Name = Destination, NumPartitions = 1, ReplicationFactor = 1 },
            new() { Name = Destination + ".dead-letter", NumPartitions = 1, ReplicationFactor = 1 }], cancellationToken: token);
    }

    public void RegisterConsumer(Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        RegisterConsumer(receive, KafkaProcessing.Defaults(1));
    }

    internal void RegisterConsumer(Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive, PartitionedProcessingOptions processing)
    {
        var native = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(Bootstrap).WithGroupId(Destination)
            .WithLoggerFactory(_logs)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest).WithOffsetCommitMode(OffsetCommitMode.Manual)
            .WithAutoOffsetStore(false).SubscribeTo(Destination);
        _transport.Consumers.Add(new(() => NativeLifecycleProxy.Wrap(native.Build(), _consumerLifetime = new()), receive, _tracing,
            new()
            {
                System = "kafka",
                Destination = Destination,
                ConsumerGroup = Destination
            }, processing, 1));
    }

    // Dekaf's partitioned runtime commits completed offsets, outside processing spans.
    public ValueTask SettleAsync(KafkaDelivery incoming, Disposition disposition, CancellationToken cancellationToken)
    {
        if (disposition.Kind != DispositionKind.Complete)
        {
            throw new NotSupportedException("Kafka has no native retry or dead-letter settlement.");
        }

        return default;
    }

    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken)
    {
        throw new NotSupportedException();
    }

    public Task DisconnectAsync(CancellationToken cancellationToken)
    {
        return Broker.StopAsync(cancellationToken);
    }

    public async Task ReconnectAsync(CancellationToken cancellationToken)
    {
        await Broker.RestartAsync(cancellationToken);
        // Process startup can finish before Kafka accepts protocol requests after a restart.
        // Synchronize with live native discovery, without changing the conformance deadline.
        await using var admin = new AdminClientBuilder().WithBootstrapServers(Bootstrap).WithLoggerFactory(_logs).Build();
        await admin.DescribeClusterAsync(new DescribeClusterOptions(), cancellationToken);
    }

    public async ValueTask VerifyShutdownAsync(CancellationToken cancellationToken)
    {
        _verifyShutdown = true;

        if (_transport.Consumers.Any(worker => !worker.Completion.IsCompletedSuccessfully))
        {
            throw new InvalidOperationException("Kafka receive loop did not drain.");
        }

        await using var reader = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(Bootstrap).WithGroupId(Destination)
            .WithLoggerFactory(_logs).WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).Build();
        await reader.InitializeAsync(cancellationToken);
        var committed = await reader.GetCommittedOffsetAsync(new(Destination, 0), cancellationToken);

        if (committed != 1 || _consumerLifetime.Commits != 1)
        {
            throw new InvalidOperationException($"Kafka shutdown did not durably commit the in-flight record: {committed}.");
        }

        _consumerLifetime.AssertClean();
        _producerLifetime.AssertClean();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _transport.DisposeAsync();
        }
        finally
        {
            try
            {
                if (_broker is not null)
                {
                    await _broker.DisposeAsync();
                }
            }
            finally
            {
                _logs.Dispose();

                if (_verifyShutdown)
                {
                    _consumerLifetime.AssertClosed();
                    _producerLifetime.AssertClosed();
                }
            }
        }
    }
}
