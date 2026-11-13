namespace Relayer;

/// <summary>The native settlement to perform after core processing completes successfully.</summary>
public enum DispositionKind
{
    /// <summary>No settlement decision was assigned. Transports must reject this value.</summary>
    Unset,
    /// <summary>Acknowledge the delivery.</summary>
    Complete,
    /// <summary>Request native redelivery after the specified delay.</summary>
    Redeliver,
    /// <summary>Dead-letter natively with error metadata.</summary>
    DeadLetter,
    /// <summary>The handler has already settled the native delivery.</summary>
    SettledByHandler,
}

/// <summary>Error metadata for a native dead-letter operation.</summary>
/// <remarks>Headers must remain valid through native settlement, after receive processing returns.</remarks>
public readonly struct DeadLetterInfo
{
    /// <summary>Creates error metadata without copying header storage.</summary>
    public DeadLetterInfo(string reason, string? description = null,
        ReadOnlyMemory<KeyValuePair<string, WireString>> headers = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        Reason = reason;
        Description = description;
        Headers = headers;
    }

    /// <summary>The actionable failure reason.</summary>
    public string Reason { get; }
    /// <summary>Optional error detail.</summary>
    public string? Description { get; }
    /// <summary>Native header names and error metadata values.</summary>
    public ReadOnlyMemory<KeyValuePair<string, WireString>> Headers { get; }
}

/// <summary>A core decision that the adapter maps to native settlement. The default value is unset and must never acknowledge a delivery.</summary>
/// <remarks>The failure pipeline must resolve capabilities and finish durable fallbacks before selecting this decision.</remarks>
public readonly struct Disposition
{
    private Disposition(DispositionKind kind, TimeSpan delay = default, DeadLetterInfo deadLetter = default, ReadOnlyMemory<KeyValuePair<string, WireString>> headers = default)
    {
        Kind = kind;
        Delay = delay;
        DeadLetter = deadLetter;
        Headers = headers;
    }

    /// <summary>The native operation to perform.</summary>
    public DispositionKind Kind { get; }
    /// <summary>Failure metadata to persist with native redelivery before releasing the delivery.</summary>
    /// <remarks>Adapters must preserve these headers through native redelivery, including when its delay is zero.</remarks>
    public ReadOnlyMemory<KeyValuePair<string, WireString>> Headers { get; }
    /// <summary>The native redelivery delay, used only for Redeliver.</summary>
    public TimeSpan Delay { get; }
    /// <summary>The error metadata, used only for DeadLetter.</summary>
    public DeadLetterInfo DeadLetter { get; }
    /// <summary>Acknowledge the delivery after durable processing.</summary>
    public static Disposition Complete => new(DispositionKind.Complete);
    /// <summary>Do not settle again after a handler uses native settlement.</summary>
    public static Disposition SettledByHandler => new(DispositionKind.SettledByHandler);

    /// <summary>Request native redelivery. Capability resolution belongs to the failure pipeline.</summary>
    public static Disposition Redeliver(TimeSpan delay, ReadOnlyMemory<KeyValuePair<string, WireString>> headers = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return new(DispositionKind.Redeliver, delay, headers: headers);
    }

    /// <summary>Request native dead-lettering with validated error metadata.</summary>
    public static Disposition DeadLettered(DeadLetterInfo info)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(info.Reason);
        return new(DispositionKind.DeadLetter, deadLetter: info);
    }
}
