using Relayer.Transports;

namespace Relayer.Kafka;

/// <summary>Describes the Kafka transport.</summary>
public static class KafkaTransport
{
    private static readonly TransportCapabilities Defaults = new()
    {
        OrderedByKey = true,
        PauseResume = true,
        Replay = true,
        ConsumptionModel = ConsumptionModel.Log,
    };

    /// <summary>Returns conservative endpoint capabilities for the transport stub (docs/decisions.md D24).</summary>
    /// <remarks>
    /// Kafka has no native delay or dead-letter operation. The native adapter must resolve configured transactions,
    /// batch counts, and message size limits before advertising them.
    /// </remarks>
    public static TransportCapabilities GetCapabilities(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return Defaults;
    }
}
