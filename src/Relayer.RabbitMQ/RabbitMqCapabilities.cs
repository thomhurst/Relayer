using Relayer.Transports;

namespace Relayer.RabbitMQ;

/// <summary>Shared conservative capabilities used by the native RabbitMQ adapter.</summary>
internal static class RabbitMqCapabilities
{
    private static readonly TransportCapabilities Defaults = new()
    {
        MaxRedeliveryDelay = TimeSpan.Zero,
        PauseResume = true,
    };

    /// <summary>Returns conservative queue capabilities before native capability evidence (docs/decisions.md D24).</summary>
    /// <remarks>
    /// Native requeue supports immediate redelivery. DLX does not retain Relayer's per-message error metadata.
    /// The native adapter must resolve delayed-message plugin support and broker message size limits before
    /// advertising them. This declaration describes AMQP queues, not RabbitMQ Streams.
    /// </remarks>
    public static TransportCapabilities GetCapabilities(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return Defaults;
    }
}
