using Relayer.AmazonSqs;
using Relayer.AzureServiceBus;
using Relayer.Kafka;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

public class TransportCapabilityStubTests
{
    [Test]
    [Arguments("orders", false)]
    [Arguments("orders.fifo", true)]
    [Arguments("https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo", true)]
    [Arguments("orders.FIFO", false)]
    [Arguments("orders.fifo-other", false)]
    public async Task Sqs_distinguishes_standard_and_fifo_endpoints(string endpoint, bool fifo)
    {
        var capabilities = AmazonSqsTransport.GetCapabilities(endpoint);

        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Queue);
        await Assert.That(capabilities.OrderedByKey).IsEqualTo(fifo);
        await Assert.That(capabilities.MaxDeliveryDelay)
            .IsEqualTo(fifo ? (TimeSpan?)null : TimeSpan.FromMinutes(15));
        await Assert.That(capabilities.MaxRedeliveryDelay).IsEqualTo(TimeSpan.FromHours(12));
        await Assert.That(capabilities.MaxBatchSize).IsEqualTo(10);
        await Assert.That(capabilities.DeadLetter).IsFalse();
        await Assert.That(capabilities.MaxMessageBytes).IsNull();
    }

    [Test]
    public async Task Kafka_declares_log_consumption_without_native_delays_or_dead_lettering()
    {
        var capabilities = KafkaTransport.GetCapabilities("events");

        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Log);
        await Assert.That(capabilities.OrderedByKey).IsTrue();
        await Assert.That(capabilities.PauseResume).IsTrue();
        await Assert.That(capabilities.Replay).IsTrue();
        await Assert.That(capabilities.MaxDeliveryDelay).IsNull();
        await Assert.That(capabilities.MaxRedeliveryDelay).IsNull();
        await Assert.That(capabilities.DeadLetter).IsFalse();
        await Assert.That(capabilities.Transactions).IsFalse();
        await Assert.That(capabilities.MaxBatchSize).IsEqualTo(1);
        await Assert.That(capabilities.MaxMessageBytes).IsNull();
    }

    [Test]
    public async Task Service_bus_declares_immediate_abandon_and_dead_letter_with_error_metadata()
    {
        var capabilities = AzureServiceBusTransport.GetCapabilities("orders");

        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Queue);
        await Assert.That(capabilities.MaxRedeliveryDelay).IsEqualTo(TimeSpan.Zero);
        await Assert.That(capabilities.DeadLetter).IsTrue();
        await Assert.That(capabilities.OrderedByKey).IsFalse();
        await Assert.That(capabilities.Transactions).IsFalse();
        await Assert.That(capabilities.MaxDeliveryDelay).IsNull();
        await Assert.That(capabilities.MaxBatchSize).IsEqualTo(1);
        await Assert.That(capabilities.MaxMessageBytes).IsNull();
    }

    [Test]
    public async Task Rabbitmq_declares_requeue_without_assuming_plugins_or_dead_letter_metadata()
    {
        var capabilities = RabbitMqTransport.GetCapabilities("orders");

        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Queue);
        await Assert.That(capabilities.MaxRedeliveryDelay).IsEqualTo(TimeSpan.Zero);
        await Assert.That(capabilities.PauseResume).IsTrue();
        await Assert.That(capabilities.DeadLetter).IsFalse();
        await Assert.That(capabilities.MaxDeliveryDelay).IsNull();
        await Assert.That(capabilities.OrderedByKey).IsFalse();
        await Assert.That(capabilities.MaxMessageBytes).IsNull();
    }

    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments(" ")]
    public async Task Stubs_reject_missing_endpoint_names(string? endpoint)
    {
        await Assert.That(() => KafkaTransport.GetCapabilities(endpoint!)).Throws<ArgumentException>();
        await Assert.That(() => AzureServiceBusTransport.GetCapabilities(endpoint!)).Throws<ArgumentException>();
        await Assert.That(() => AmazonSqsTransport.GetCapabilities(endpoint!)).Throws<ArgumentException>();
        await Assert.That(() => RabbitMqTransport.GetCapabilities(endpoint!)).Throws<ArgumentException>();
    }
}
