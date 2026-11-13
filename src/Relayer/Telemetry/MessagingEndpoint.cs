namespace Relayer.Transports;

/// <summary>Constant endpoint attributes supplied by a native adapter at startup.</summary>
public sealed class MessagingEndpoint
{
    /// <summary>The semantic-conventions system value, such as kafka or servicebus.</summary>
    public required string System { get; init; }
    /// <summary>The native destination in the system's format.</summary>
    public required string Destination { get; init; }
    /// <summary>A low-cardinality template, when available.</summary>
    public string? DestinationTemplate { get; init; }
    /// <summary>Whether this destination is temporary.</summary>
    public bool Temporary { get; init; }
    /// <summary>Whether this destination is anonymous.</summary>
    public bool Anonymous { get; init; }
    /// <summary>The native consumer group, when applicable.</summary>
    public string? ConsumerGroup { get; init; }
    /// <summary>The native subscription, when applicable.</summary>
    public string? Subscription { get; init; }
    /// <summary>Additional convention-defined native endpoint attributes, precomputed by the adapter.</summary>
    public ReadOnlyMemory<KeyValuePair<string, object?>> Attributes { get; init; }
    /// <summary>The native client identifier.</summary>
    public string? ClientId { get; init; }
    /// <summary>The broker host.</summary>
    public string? ServerAddress { get; init; }
    /// <summary>The broker port.</summary>
    public int? ServerPort { get; init; }
}
