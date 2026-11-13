using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

public class RabbitMqCapabilityTests
{
    [Test]
    public async Task Unconfigured_native_adapter_preserves_conservative_queue_capabilities()
    {
        await using var transport = new RabbitMqNativeTransport("rabbit", new(), []);
        var capabilities = transport.GetCapabilities("orders");
        await Assert.That(capabilities.ConsumptionModel).IsEqualTo(ConsumptionModel.Queue);
        await Assert.That(capabilities.MaxRedeliveryDelay).IsEqualTo(TimeSpan.Zero);
        await Assert.That(capabilities.PauseResume).IsTrue();
        await Assert.That(capabilities.DeadLetter).IsFalse();
        await Assert.That(capabilities.MaxDeliveryDelay).IsNull();
        await Assert.That(capabilities.OrderedByKey).IsFalse();
        await Assert.That(capabilities.MaxMessageBytes).IsNull();
        await Assert.That(() => transport.GetCapabilities(" ")).Throws<ArgumentException>();
    }
}
