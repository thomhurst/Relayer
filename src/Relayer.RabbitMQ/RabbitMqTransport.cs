using Relayer.Transports;

namespace Relayer.RabbitMQ;

/// <summary>Describes the RabbitMq transport.</summary>
public static class RabbitMqTransport
{
    private static readonly TransportCapabilities Defaults = new()
    {
        MaxRedeliveryDelay = TimeSpan.Zero,
        PauseResume = true,
    };

    /// <summary>Returns conservative queue capabilities for the transport stub (docs/decisions.md D24).</summary>
    /// <remarks>
    /// Native requeue supports immediate redelivery. DLX does not retain Relayer's per-message error metadata.
    /// The native adapter must resolve delayed-message plugin support and broker message size limits before
    /// advertising them. This stub describes AMQP queues, not RabbitMQ Streams.
    /// </remarks>
    public static TransportCapabilities GetCapabilities(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return Defaults;
    }
}
