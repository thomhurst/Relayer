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
    public async Task Provision_restores_an_administration_created_queue_lost_on_emulator_restart()
    {
        await using var broker = await ServiceBusReceiveBroker.StartAsync([]);
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var queue = "reprovision-" + Guid.NewGuid().ToString("N");
        await using var client = new ServiceBusClient(broker.ConnectionString);
        var admin = new ServiceBusAdministrationClient(broker.AdministrationConnectionString);
        var options = new AzureServiceBusTransportOptions { Client = client };
        options.Queues.Add(new(queue) { DefaultMessageTimeToLive = TimeSpan.FromHours(1),
            MaxSizeInMegabytes = 100, DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(10) });
        await using var transport = new ServiceBusSendTransport("reprovision", client, false, options,
            [queue], TopologyMode.Provision, admin);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
        await Assert.That((await admin.QueueExistsAsync(queue, token)).Value).IsTrue();
        await broker.DisconnectAsync(token);
        await broker.ReconnectAsync(token);
        // This queue was never in startup config. Assert actual loss before testing restoration.
        await Assert.That((await admin.QueueExistsAsync(queue, token)).Value).IsFalse();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
        await Assert.That((await admin.QueueExistsAsync(queue, token)).Value).IsTrue();
        await transport.SendAsync(new(queue, new byte[] { 42 }, ReadOnlyMemory<KeyValuePair<string, WireString>>.Empty), token);
        await using var receiver = client.CreateReceiver(queue);
        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(30), token)
            ?? throw new InvalidOperationException("Restored queue did not deliver.");
        await Assert.That(message.Body.ToMemory().ToArray()).IsEquivalentTo(new byte[] { 42 });
        await receiver.CompleteMessageAsync(message, token);
    }

    [Test]
    public async Task Service_bus_emulator_passes_queue_and_session_transport_contracts()
    {
        // Session ordering runs separately; native shutdown evidence is tracked in #384.
        var destinations = Enum.GetValues<TransportConformanceCase>()
            .Where(scenario => scenario is not (TransportConformanceCase.OrderingByKey or TransportConformanceCase.CleanShutdown))
            .ToDictionary(scenario => scenario, _ => "conformance-" + Guid.NewGuid().ToString("N"));
        var sessionDestination = "conformance-session-" + Guid.NewGuid().ToString("N");
        // Provisioning owns its deadline; broker assertions get their full operation budget.
        await using var broker = await ServiceBusReceiveBroker.StartAsync(destinations.Values.ToArray(), [sessionDestination]);
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(8));
        var token = timeout.Token;
        var destination = "";
        var queues = new TransportConformanceSuite<ProcessMessageEventArgs, ServiceBusBinding>(
            (tracing, cancellation) => ServiceBusConformanceFixture<ProcessMessageEventArgs>.CreateAsync(broker, tracing, false, destination, cancellation),
            TimeSpan.FromMinutes(2));
        foreach (var scenario in destinations.Keys)
        {
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
    public TimeSpan? OutageSendTimeout => TimeSpan.FromSeconds(5);
    public bool IsExpectedOutageFailure(Exception error) => error is ServiceBusException { IsTransient: true };
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
                async (args, token) => await receive((TIncoming)(object)args, token)));
        }
        else
        {
            var processor = _client.CreateProcessor(Destination, _declaration.Options.ProcessorOptions);
            _transport.Consumers.Add(new(_declaration, processor,
                async (args, token) => await receive((TIncoming)(object)args, token)));
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
