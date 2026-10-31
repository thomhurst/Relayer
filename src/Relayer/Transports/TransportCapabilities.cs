namespace Relayer.Transports;

/// <summary>Native capabilities a transport declares. Validated against configuration at startup (docs/decisions.md D14).</summary>
[Flags]
public enum TransportCapabilities
{
    /// <summary>No optional capabilities.</summary>
    None = 0,

    /// <summary>The broker can delay delivery natively.</summary>
    DelayedDelivery = 1 << 0,

    /// <summary>The broker has a native dead-letter mechanism.</summary>
    DeadLetter = 1 << 1,

    /// <summary>The broker can preserve order per key (partition key, session, message group).</summary>
    OrderedByKey = 1 << 2,

    /// <summary>The client supports native batch send or receive.</summary>
    Batching = 1 << 3,

    /// <summary>The broker supports native transactions.</summary>
    Transactions = 1 << 4,

    /// <summary>Consumers can be paused and resumed natively.</summary>
    PauseResume = 1 << 5,

    /// <summary>The broker can replay from a position (offset or timestamp).</summary>
    Replay = 1 << 6,
}
