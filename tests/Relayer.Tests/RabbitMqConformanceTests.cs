using System.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Relayer.RabbitMQ;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Tests;

public class RabbitMqConformanceTests
{
    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.Retry)]
    [Arguments(TransportConformanceCase.FailureHandlingFailure)]
    [Arguments(TransportConformanceCase.DeadLetter)]
    [Arguments(TransportConformanceCase.Delay)]
    [Arguments(TransportConformanceCase.RedeliveryDelay)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.Reconnect)]
    [Arguments(TransportConformanceCase.CleanShutdown)]
    [Arguments(TransportConformanceCase.SendDuringOutage)]
    public async Task RabbitMQ_passes(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<RabbitMqDelivery, RabbitMqMessageBinding>(
            // These two stop the broker.
            (tracing, token) => RabbitMqFixture.CreateAsync(tracing, token,
                scenario is TransportConformanceCase.Reconnect or TransportConformanceCase.SendDuringOutage), TimeSpan.FromMinutes(3));
        var result = await suite.RunAsync(scenario);
        await Assert.That(result.Skipped).IsEqualTo(scenario is TransportConformanceCase.DeadLetter or TransportConformanceCase.Delay
            or TransportConformanceCase.RedeliveryDelay or TransportConformanceCase.OrderingByKey);
    }
}

internal sealed class RabbitMqFixture : ITransportConformanceFixture<RabbitMqDelivery>, ITransportShutdownConformanceFixture
{
    private readonly RabbitMqTopologyTests.Broker _broker;
    private readonly RabbitMqNativeTransport _transport;
    private readonly MessageTracing _tracing;
    private readonly RabbitMqSettlement _settlement;
    private readonly ConnectionFactory _factory;
    private readonly ActivityListener _nativeTracing = new()
    {
        ShouldListenTo = static source => source.Name is "RabbitMQ.Client.Publisher" or "RabbitMQ.Client.Subscriber",
        Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    };
    private RabbitMqConsumer? _consumer;
    private int _settlements;
    private int _closed;
    public ConformanceLogCapture Logs { get; } = new();
    public string Destination => "conformance";
    public string SendDestinationName => ":conformance";
    public string ProcessDestinationName => ":conformance:conformance";
    public string MessagingSystem => "rabbitmq";
    public string SettlementOperation => "ack";
    public HeaderNames HeaderNames { get; } = new("cloudEvents:");
    public ITransport Transport => _transport;
    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(2);
    public bool IsExpectedOutageFailure(Exception error) => error is AlreadyClosedException or OperationInterruptedException or IOException;

    private RabbitMqFixture(RabbitMqTopologyTests.Broker broker, MessageTracing tracing)
    {
        _broker = broker;
        ActivitySource.AddActivityListener(_nativeTracing);
        _tracing = tracing;
        _factory = broker.Factory();
        _factory.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(200);
        _factory.RequestedConnectionTimeout = TimeSpan.FromSeconds(2);
        _transport = new("rabbit", new() { ConnectionFactory = _factory, PublisherCount = 1 }, [Destination], tracing);
        _settlement = new(tracing, new() { System = "rabbitmq", Destination = ProcessDestinationName });
    }
    internal static async ValueTask<ITransportConformanceFixture<RabbitMqDelivery>> CreateAsync(MessageTracing tracing, CancellationToken token, bool dedicated)
    {
        var broker = await RabbitMqTopologyTests.Broker.StartAsync(token, dedicated: dedicated);
        var fixture = new RabbitMqFixture(broker, tracing);
        try
        {
            await using var connection = await fixture._factory.CreateConnectionAsync(token);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
            await channel.QueueDeclareAsync(fixture.Destination, true, false, false, cancellationToken: token);
            return fixture;
        }
        catch { await fixture.DisposeAsync(); throw; }
    }
    public void RegisterConsumer(Func<RabbitMqDelivery, CancellationToken, ValueTask<Disposition>> receive)
    {
        var options = new RabbitMqConsumerOptions();
        var declaration = new ConsumerDefinition<RabbitMqConsumerOptions>(Destination, options, [], 1, 1, TimeSpan.Zero);
        _consumer = new(declaration, options, receive, _tracing);
        _transport.Consumers.Add(_consumer);
    }
    public async ValueTask SettleAsync(RabbitMqDelivery delivery, Disposition disposition, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _closed) != 0) throw new InvalidOperationException("Settlement after shutdown.");
        await _settlement.SettleAsync(delivery, disposition, cancellationToken);
        Interlocked.Increment(ref _settlements);
    }
    public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
    public Task DisconnectAsync(CancellationToken cancellationToken) => _broker.CommandAsync(["rabbitmqctl", "stop_app"], cancellationToken);
    public async Task ReconnectAsync(CancellationToken cancellationToken)
    {
        await _broker.CommandAsync(["rabbitmqctl", "start_app"], cancellationToken);
        if (_consumer?.Completion.IsCompleted == false)
            while (!_transport.IsConnected) await Task.Delay(20, cancellationToken);
        // Connection recovery precedes consumer recovery. A passive native query synchronizes broker readiness;
        // the subsequent conformance delivery proves that the registered consumer recovers too.
        await using var connection = await _factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        await channel.QueueDeclarePassiveAsync(Destination, cancellationToken);
    }
    public async ValueTask VerifyShutdownAsync(CancellationToken cancellationToken)
    {
        await Assert.That(_consumer!.Completion.IsCompletedSuccessfully).IsTrue();
        await Assert.That(Volatile.Read(ref _settlements)).IsEqualTo(1);
        Volatile.Write(ref _closed, 1);
        await using var connection = await _factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        await Assert.That((await channel.QueueDeclarePassiveAsync(Destination, cancellationToken)).MessageCount).IsEqualTo(0u);
    }
    public async ValueTask DisposeAsync()
    {
        try { await _transport.DisposeAsync(); }
        finally
        {
            _nativeTracing.Dispose();
            await _broker.DisposeAsync();
        }
    }
}
