namespace Relayer;

/// <summary>Declared, low-cardinality metric dimensions. Create one recorder per endpoint and operation at startup.</summary>
public sealed class EndpointTelemetryOptions
{
    /// <summary>The native messaging system, for example kafka, servicebus or relayer.inmemory.</summary>
    public required string System { get; init; }
    /// <summary>The system's operation name, for example process, send, complete or ack.</summary>
    public string OperationName { get; init; } = "process";
    /// <summary>The declared destination. Do not supply a dynamic destination or message key.</summary>
    public string? Destination { get; init; }
    /// <summary>The declared consumer group, where applicable.</summary>
    public string? ConsumerGroup { get; init; }
    /// <summary>The declared subscription, where applicable.</summary>
    public string? Subscription { get; init; }
    /// <summary>The broker's known address, without reverse DNS lookup.</summary>
    public string? ServerAddress { get; init; }
    /// <summary>The broker's known port.</summary>
    public int? ServerPort { get; init; }
}
