using Relayer.Transports;

namespace Relayer.Tests;

public class TransportCapabilitiesTests
{
    [Test]
    public async Task Defaults_do_not_advertise_optional_native_features()
    {
        var capabilities = new TransportCapabilities();

        await Assert.That(capabilities.MaxDeliveryDelay).IsNull();
        await Assert.That(capabilities.MaxRedeliveryDelay).IsNull();
        await Assert.That(capabilities.DeadLetter).IsFalse();
        await Assert.That(capabilities.OrderedByKey).IsFalse();
        await Assert.That(capabilities.MaxBatchSize).IsEqualTo(1);
        await Assert.That(capabilities.Transactions).IsFalse();
        await Assert.That(capabilities.PauseResume).IsFalse();
        await Assert.That(capabilities.Replay).IsFalse();
        await Assert.That(capabilities.MaxMessageBytes).IsNull();
        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Queue);
    }

    [Test]
    public async Task Core_reads_distinct_endpoint_declarations_through_the_transport_contract()
    {
        await using ITransport transport = new DeclaredTransport();

        var queue = transport.GetCapabilities("orders");
        var log = transport.GetCapabilities("events");

        await Assert.That(queue.MaxDeliveryDelay).IsEqualTo(TimeSpan.FromMinutes(15));
        await Assert.That(queue.MaxRedeliveryDelay).IsEqualTo(TimeSpan.FromHours(12));
        await Assert.That(queue.DeadLetter).IsTrue();
        await Assert.That(queue.OrderedByKey).IsFalse();
        await Assert.That(queue.MaxBatchSize).IsEqualTo(10);
        await Assert.That(queue.MaxMessageBytes).IsEqualTo(262_144L);
        await Assert.That(queue.ConsumptionModel).IsEqualTo(ConsumptionModel.Queue);
        await Assert.That(log.MaxDeliveryDelay).IsNull();
        await Assert.That(log.MaxRedeliveryDelay).IsNull();
        await Assert.That(log.DeadLetter).IsFalse();
        await Assert.That(log.OrderedByKey).IsTrue();
        await Assert.That(log.Transactions).IsTrue();
        await Assert.That(log.PauseResume).IsTrue();
        await Assert.That(log.Replay).IsTrue();
        await Assert.That(log.ConsumptionModel).IsEqualTo(ConsumptionModel.Log);
    }

    [Test]
    public async Task Immediate_redelivery_is_distinct_from_no_native_redelivery()
    {
        var none = new TransportCapabilities();
        var immediate = none with { MaxRedeliveryDelay = TimeSpan.Zero };

        await Assert.That(none.MaxRedeliveryDelay).IsNull();
        await Assert.That(immediate.MaxRedeliveryDelay).IsEqualTo(TimeSpan.Zero);
        await Assert.That(none == immediate).IsFalse();
    }

    [Test]
    public async Task Endpoint_records_can_be_specialized_without_mutating_the_original()
    {
        var standard = new TransportCapabilities { MaxBatchSize = 10 };
        var ordered = standard with { OrderedByKey = true };

        await Assert.That(standard.OrderedByKey).IsFalse();
        await Assert.That(ordered.OrderedByKey).IsTrue();
        await Assert.That(ordered.MaxBatchSize).IsEqualTo(10);
        await Assert.That(standard).IsEqualTo(new TransportCapabilities { MaxBatchSize = 10 });
    }

    private sealed class DeclaredTransport : ITransport
    {
        public string Name => "declared";

        public TransportCapabilities GetCapabilities(string endpoint) => endpoint switch
        {
            "orders" => new()
            {
                MaxDeliveryDelay = TimeSpan.FromMinutes(15),
                MaxRedeliveryDelay = TimeSpan.FromHours(12),
                DeadLetter = true,
                MaxBatchSize = 10,
                MaxMessageBytes = 262_144,
            },
            "events" => new()
            {
                OrderedByKey = true,
                Transactions = true,
                PauseResume = true,
                Replay = true,
                ConsumptionModel = ConsumptionModel.Log,
            },
            _ => throw new ArgumentException("Unknown endpoint.", nameof(endpoint)),
        };

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
