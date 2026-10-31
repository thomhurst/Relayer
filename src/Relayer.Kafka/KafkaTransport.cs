using Relayer.Transports;

namespace Relayer.Kafka;

/// <summary>Describes the Kafka transport.</summary>
public static class KafkaTransport
{
    /// <summary>
    /// The native capabilities of this transport (docs/decisions.md D14).
    /// Kafka has no native delay or dead-letter queue; Relayer uses the outbox and a dead-letter topic.
    /// </summary>
    public const TransportCapabilities Capabilities =
        TransportCapabilities.OrderedByKey | TransportCapabilities.Batching | TransportCapabilities.Transactions | TransportCapabilities.PauseResume | TransportCapabilities.Replay;
}
