using Relayer.Transports;

namespace Relayer.AzureServiceBus;

/// <summary>Describes the AzureServiceBus transport.</summary>
public static class AzureServiceBusTransport
{
    /// <summary>
    /// The native capabilities of this transport (docs/decisions.md D14).
    /// Ordering uses sessions; delays use ScheduledEnqueueTime.
    /// </summary>
    public const TransportCapabilities Capabilities =
        TransportCapabilities.DelayedDelivery | TransportCapabilities.DeadLetter | TransportCapabilities.OrderedByKey | TransportCapabilities.Batching | TransportCapabilities.Transactions | TransportCapabilities.PauseResume;
}
