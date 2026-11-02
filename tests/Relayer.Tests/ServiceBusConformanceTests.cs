using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using System.Diagnostics;
using Relayer.AzureServiceBus;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Tests;

public sealed class ServiceBusConformanceTests
{
    [Test]
    public async Task Service_bus_emulator_passes_queue_and_session_transport_contracts()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));
        var token = timeout.Token;
        var destinations = Enum.GetValues<TransportConformanceCase>().Where(scenario => scenario != TransportConformanceCase.OrderingByKey)
            .ToDictionary(scenario => scenario, _ => "conformance-" + Guid.NewGuid().ToString("N"));
        var sessionDestination = "conformance-session-" + Guid.NewGuid().ToString("N");
        await using var broker = await ServiceBusReceiveBroker.StartAsync(token, destinations.Values.ToArray(), [sessionDestination]);
        var destination = "";
        var queues = new TransportConformanceSuite<ProcessMessageEventArgs, ServiceBusBinding>(
            (tracing, cancellation) => ServiceBusConformanceFixture<ProcessMessageEventArgs>.CreateAsync(broker, tracing, false, destination, cancellation),
            TimeSpan.FromMinutes(2));
        foreach (var scenario in Enum.GetValues<TransportConformanceCase>())
        {
            if (scenario == TransportConformanceCase.OrderingByKey) continue;
            destination = destinations[scenario];
            var result = await queues.RunAsync(scenario, token);
            if (scenario == TransportConformanceCase.RedeliveryDelay)
            {
                await Assert.That(result.Skipped).IsTrue();
                await Assert.That(result.SkipReason).IsEqualTo("No positive native redelivery delay.");
            }
            else await Assert.That(result.Skipped).IsFalse();
        }
        var sessions = new TransportConformanceSuite<ProcessSessionMessageEventArgs, ServiceBusSessionBinding>(
            (tracing, cancellation) => ServiceBusConformanceFixture<ProcessSessionMessageEventArgs>.CreateAsync(broker, tracing, true, sessionDestination, cancellation),
            TimeSpan.FromMinutes(2));
        await Assert.That((await sessions.RunAsync(TransportConformanceCase.OrderingByKey, token)).Skipped).IsFalse();
    }
}

internal sealed class ServiceBusConformanceFixture<TIncoming> : ITransportConformanceFixture<TIncoming>
{
    private readonly ServiceBusReceiveBroker _broker;
    private readonly ServiceBusClient _client;
    private readonly ServiceBusAdministrationClient _admin;
    private readonly ServiceBusSendTransport _transport;
    private readonly ServiceBusSettlement _settlement;
    private readonly ConsumerDefinition<ServiceBusConsumerOptions> _declaration;
    private readonly bool _sessions;
    private readonly ActivityListener _nativeInstrumentation;
    public string Destination { get; }
    public ITransport Transport => _transport;
    public string MessagingSystem => "servicebus";
    public string SettlementOperation => "complete";
    public HeaderNames HeaderNames { get; } = new("cloudEvents:");

    private ServiceBusConformanceFixture(ServiceBusReceiveBroker broker, MessageTracing tracing, bool sessions, string destination)
    {
        Destination = destination;
        _broker = broker;
        _sessions = sessions;
        _nativeInstrumentation = new()
        {
            ShouldListenTo = static source => source.Name.StartsWith("Azure.Messaging.ServiceBus", StringComparison.Ordinal),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(_nativeInstrumentation);
        _client = new(broker.ConnectionString);
        _admin = new(broker.AdministrationConnectionString);
        var options = new ServiceBusConsumerOptions();
        if (sessions) options.SessionProcessorOptions = new() { AutoCompleteMessages = false, MaxConcurrentSessions = 1 };
        _declaration = new(Destination, options, [], 1, 1, TimeSpan.FromMilliseconds(100));
        var native = new AzureServiceBusTransportOptions { Client = _client };
        native.Queues.Add(new(Destination) { RequiresSession = sessions, DefaultMessageTimeToLive = TimeSpan.FromHours(1) });
        _transport = new("conformance", _client, true, native, [Destination], TopologyMode.Provision, _admin, tracing,
            consumers: [_declaration]);
        _settlement = new(tracing, new() { System = "servicebus", Destination = Destination });
    }

    internal static ValueTask<ITransportConformanceFixture<TIncoming>> CreateAsync(ServiceBusReceiveBroker broker,
        MessageTracing tracing, bool sessions, string destination, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return new(new ServiceBusConformanceFixture<TIncoming>(broker, tracing, sessions, destination));
    }

    public void RegisterConsumer(Func<TIncoming, CancellationToken, ValueTask<Disposition>> receive)
    {
        if (_sessions)
        {
            var processor = _client.CreateSessionProcessor(Destination, _declaration.Options.SessionProcessorOptions!);
            _transport.Consumers.Add(new(_declaration, processor,
                (args, token) => receive((TIncoming)(object)args, token)));
        }
        else
        {
            var processor = _client.CreateProcessor(Destination, _declaration.Options.ProcessorOptions);
            _transport.Consumers.Add(new(_declaration, processor,
                (args, token) => receive((TIncoming)(object)args, token)));
        }
    }

    public ValueTask SettleAsync(TIncoming incoming, Disposition disposition, CancellationToken cancellationToken) =>
        _sessions ? _settlement.SettleAsync((ProcessSessionMessageEventArgs)(object)incoming!, disposition, cancellationToken)
            : _settlement.SettleAsync((ProcessMessageEventArgs)(object)incoming!, disposition, cancellationToken);

    public async ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken)
    {
        await using var receiver = _client.CreateReceiver(Destination, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        // The fixture owns and deletes this queue. Inspect the DLQ without taking an unnecessary settlement lock.
        var message = await receiver.PeekMessageAsync(cancellationToken: cancellationToken)
            ?? throw new InvalidOperationException("Native Service Bus DLQ is empty.");
        var result = new ConformanceDeadLetter(message.Body.ToMemory().ToArray(),
            message.ApplicationProperties.ToDictionary(property => property.Key, property => property.Value.ToString()!),
            message.DeadLetterReason, message.DeadLetterErrorDescription);
        return result;
    }

    public Task DisconnectAsync(CancellationToken cancellationToken) => _broker.DisconnectAsync(cancellationToken);
    public Task ReconnectAsync(CancellationToken cancellationToken) => _broker.ReconnectAsync(cancellationToken);
    public async ValueTask DisposeAsync()
    {
        try { await _transport.DisposeAsync(); await _admin.DeleteQueueAsync(Destination); }
        finally { _nativeInstrumentation.Dispose(); }
    }
}
