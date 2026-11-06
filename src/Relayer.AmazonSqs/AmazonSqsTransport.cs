using Relayer.Transports;

namespace Relayer.AmazonSqs;

/// <summary>Describes the AmazonSqs transport.</summary>
public static class AmazonSqsTransport
{
    private static readonly TransportCapabilities Standard = new()
    {
        MaxDeliveryDelay = TimeSpan.FromMinutes(15),
        MaxRedeliveryDelay = TimeSpan.FromHours(12),
        MaxBatchSize = 10,
    };

    private static readonly TransportCapabilities Fifo = Standard with
    {
        MaxDeliveryDelay = null,
        OrderedByKey = true,
    };

    /// <summary>Returns conservative SQS queue capabilities, independently of a registered native adapter (docs/decisions.md D24).</summary>
    /// <remarks>
    /// Accepts a queue name or URL. FIFO queues require the case-sensitive .fifo suffix and do not support
    /// per-message delivery timers. Redrive cannot dead-letter an individual message on demand with error metadata.
    /// The native adapter must resolve the queue's configured message size limit. Visibility changes remain subject
    /// to the native twelve-hour deadline measured from receipt, rather than restarting that deadline.
    /// </remarks>
    public static TransportCapabilities GetCapabilities(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return endpoint.EndsWith(".fifo", StringComparison.Ordinal) ? Fifo : Standard;
    }
}
