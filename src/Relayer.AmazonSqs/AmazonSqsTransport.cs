using Relayer.Transports;

namespace Relayer.AmazonSqs;

/// <summary>Describes the AmazonSqs transport.</summary>
public static class AmazonSqsTransport
{
    /// <summary>
    /// The native capabilities of this transport (docs/decisions.md D14).
    /// Native delay is limited to 15 minutes; ordering requires FIFO queues.
    /// </summary>
    public const TransportCapabilities Capabilities =
        TransportCapabilities.DelayedDelivery | TransportCapabilities.DeadLetter | TransportCapabilities.OrderedByKey | TransportCapabilities.Batching;
}
