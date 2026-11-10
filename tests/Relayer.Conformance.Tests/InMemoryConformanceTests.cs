using System.Buffers;
using Relayer.InMemory;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Conformance.Tests;

public class InMemoryConformanceTests
{
    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.Retry)]
    [Arguments(TransportConformanceCase.DeadLetter)]
    [Arguments(TransportConformanceCase.Delay)]
    [Arguments(TransportConformanceCase.RedeliveryDelay)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.Reconnect)]
    [Arguments(TransportConformanceCase.CleanShutdown)]
    public async Task Queue_passes(TransportConformanceCase scenario)
    {
        var result = await Suite(log: false).RunAsync(scenario);
        await Assert.That(result.Skipped).IsFalse();
    }

    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.Retry)]
    [Arguments(TransportConformanceCase.DeadLetter)]
    [Arguments(TransportConformanceCase.Delay)]
    [Arguments(TransportConformanceCase.RedeliveryDelay)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.Reconnect)]
    [Arguments(TransportConformanceCase.CleanShutdown)]
    public async Task Log_passes(TransportConformanceCase scenario)
    {
        var result = await Suite(log: true).RunAsync(scenario);
        await Assert.That(result.Skipped).IsFalse();
    }

    [Test]
    [Arguments(TransportConformanceCase.Retry)]
    [Arguments(TransportConformanceCase.DeadLetter)]
    [Arguments(TransportConformanceCase.Delay)]
    [Arguments(TransportConformanceCase.RedeliveryDelay)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    public async Task Unsupported_native_capabilities_report_explicit_skips(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new InMemoryFixture(tracing, capabilities: new())));
        var result = await suite.RunAsync(scenario);
        await Assert.That(result.Skipped).IsTrue();
        await Assert.That(result.SkipReason).IsNotNull();
    }

    [Test]
    public async Task Immediate_only_redelivery_skips_delay_but_bounded_delivery_delay_runs()
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new InMemoryFixture(tracing,
                capabilities: new() { MaxRedeliveryDelay = TimeSpan.Zero, MaxDeliveryDelay = TimeSpan.FromMilliseconds(100) })));
        await Assert.That((await suite.RunAsync(TransportConformanceCase.Retry)).Skipped).IsFalse();
        await Assert.That((await suite.RunAsync(TransportConformanceCase.RedeliveryDelay)).Skipped).IsTrue();
        await Assert.That((await suite.RunAsync(TransportConformanceCase.Delay)).Skipped).IsFalse();
    }

    [Test]
    public async Task Missing_native_send_tracing_fails_instead_of_skipping()
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding>((_, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new InMemoryFixture(null)));
        await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.TracePropagation))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Overwritten_creation_context_fails_instead_of_skipping()
    {
        var suite = new TransportConformanceSuite<InMemoryDelivery, BrokenCreationBinding>((tracing, _) =>
            new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new InMemoryFixture(tracing)));
        await Assert.That(async () => await suite.RunAsync(TransportConformanceCase.CreationContextCopies))
            .Throws<InvalidOperationException>();
    }

    private static TransportConformanceSuite<InMemoryDelivery, InMemoryMessageBinding> Suite(bool log) =>
        new((tracing, _) => new ValueTask<ITransportConformanceFixture<InMemoryDelivery>>(new InMemoryFixture(tracing, log)));

    private readonly struct BrokenCreationBinding : IMessageBinding<InMemoryDelivery>
    {
        public static ReadOnlySequence<byte> GetBody(InMemoryDelivery message) => InMemoryMessageBinding.GetBody(message);
        public static int? GetDeliveryCount(InMemoryDelivery message) => message.Attempt;
        public static bool TryGetHeader(InMemoryDelivery message, string name, out WireString value)
        {
            if (name == "traceparent")
            {
                value = new WireString("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01");
                return true;
            }
            return InMemoryMessageBinding.TryGetHeader(message, name, out value);
        }
    }
}

internal sealed class InMemoryFixture : ITransportConformanceFixture<InMemoryDelivery>, ITransportShutdownConformanceFixture
{
    private readonly InMemoryTransport _transport;
    private readonly bool _log;
    private int _completed;
    public ConformanceLogCapture Logs { get; } = new();

    internal InMemoryFixture(MessageTracing? tracing, bool log = false, TransportCapabilities? capabilities = null)
    {
        _log = log;
        _transport = new("conformance", new Dictionary<string, InMemoryEndpointOptions>
        {
            [Destination] = new()
            {
                Partitions = capabilities is null ? 2 : 1,
                Capabilities = capabilities ?? new()
                {
                    ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                    OrderedByKey = true,
                    DeadLetter = true,
                    MaxDeliveryDelay = TimeSpan.FromMinutes(1),
                    MaxRedeliveryDelay = TimeSpan.FromMinutes(1),
                },
            },
        }, tracing: tracing);
    }

    public ITransport Transport => _transport;
    public string Destination => "conformance";
    public string MessagingSystem => "relayer.inmemory";
    public string SettlementOperation => "complete";
    public HeaderNames HeaderNames { get; } = new();
    public void RegisterConsumer(Func<InMemoryDelivery, CancellationToken, ValueTask<Disposition>> receive) =>
        _transport.RegisterConsumer(Destination, (delivery, token) => receive(delivery, token), _log ? "conformance" : null, 2);
    public ValueTask SettleAsync(InMemoryDelivery incoming, Disposition disposition, CancellationToken cancellationToken)
    {
        switch (disposition.Kind)
        {
            case DispositionKind.Complete: incoming.Complete(); Interlocked.Increment(ref _completed); break;
            case DispositionKind.Redeliver: incoming.Redeliver(disposition.Delay); break;
            case DispositionKind.DeadLetter: incoming.DeadLetter(disposition.DeadLetter); break;
            default: throw new InvalidOperationException("Unexpected conformance disposition.");
        }
        return default;
    }
    public async ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken)
    {
        var dead = await _transport.DeadLetters(Destination).ReadAsync(cancellationToken);
        return new(dead.Body, dead.Headers.ToArray().ToDictionary(header => header.Key, header => header.Value.ToString()),
            dead.Error.Reason, dead.Error.Description);
    }
    public Task DisconnectAsync(CancellationToken cancellationToken) => _transport.StopAsync(cancellationToken);
    public Task ReconnectAsync(CancellationToken cancellationToken) => _transport.StartAsync(cancellationToken);
    public ValueTask VerifyShutdownAsync(CancellationToken cancellationToken)
    {
        // Completion is the production native-loop task; it includes handler execution and settlement.
        if (!_transport.Completion.IsCompletedSuccessfully || Volatile.Read(ref _completed) != 1)
            throw new InvalidOperationException("In-memory shutdown did not drain and complete the in-flight delivery.");
        return ValueTask.CompletedTask;
    }
    // InMemoryTransport has no logger or external native receiver. Its completed loop is its closure boundary.
    public ValueTask DisposeAsync() => _transport.DisposeAsync();
}
