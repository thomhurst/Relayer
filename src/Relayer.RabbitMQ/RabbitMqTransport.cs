using Relayer.Transports;

namespace Relayer.RabbitMQ;

/// <summary>Describes the RabbitMq transport.</summary>
public static class RabbitMqTransport
{
    /// <summary>
    /// The native capabilities of this transport (docs/decisions.md D14).
    /// Dead-lettering uses DLX. Native delay needs the delayed-message plugin and is detected at startup.
    /// </summary>
    public const TransportCapabilities Capabilities =
        TransportCapabilities.DeadLetter | TransportCapabilities.PauseResume;
}
