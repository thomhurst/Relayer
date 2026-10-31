using RabbitMQ.Client;

namespace Relayer.RabbitMQ;

/// <summary>Options for the RabbitMq transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class RabbitMqTransportOptions
{
    /// <summary>Native connection factory, passed straight to RabbitMQ.Client.</summary>
    public ConnectionFactory ConnectionFactory { get; set; } = new();
}
