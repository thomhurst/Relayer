namespace Relayer.Transports;

/// <summary>
/// The native capabilities of one endpoint (docs/decisions.md D24).
/// Transports resolve these values at startup; the core uses them to select native operations or fallbacks.
/// </summary>
public sealed record TransportCapabilities
{
    /// <summary>The maximum native delay for an individual send; null means no native scheduled send.</summary>
    public TimeSpan? MaxDeliveryDelay { get; init; }

    /// <summary>
    /// The maximum native delay when redelivering a received message; zero means immediate redelivery only,
    /// and null means no native redelivery.
    /// </summary>
    public TimeSpan? MaxRedeliveryDelay { get; init; }

    /// <summary>Whether a specific message can be dead-lettered on demand with Relayer's error metadata.</summary>
    public bool DeadLetter { get; init; }

    /// <summary>Whether the endpoint preserves native order per key, session, or message group.</summary>
    public bool OrderedByKey { get; init; }

    /// <summary>Whether this endpoint supports keyed native null-value records.</summary>
    public bool Tombstones { get; init; }

    /// <summary>The maximum native batch message count; 1 declares no native batching.</summary>
    /// <remarks>Native byte limits still apply independently of this count.</remarks>
    public int MaxBatchSize { get; init; } = 1;

    /// <summary>Whether native transactions are available on this endpoint.</summary>
    public bool Transactions { get; init; }

    /// <summary>Whether native consumption can be paused and resumed.</summary>
    public bool PauseResume { get; init; }

    /// <summary>Whether consumption can replay from a native position or timestamp.</summary>
    public bool Replay { get; init; }

    /// <summary>The native message size limit in bytes; null means the limit has not been declared.</summary>
    /// <remarks>Null does not mean unlimited. Native envelope overhead and batch byte limits still apply.</remarks>
    public long? MaxMessageBytes { get; init; }

    /// <summary>Whether consumption settles individual queue messages or advances through a log.</summary>
    public ConsumptionModel ConsumptionModel { get; init; } = ConsumptionModel.Queue;
}
