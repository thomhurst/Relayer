namespace Relayer.Transports;

/// <summary>A native delivery feature reused with the receive context.</summary>
/// <remarks>Do not retain this feature or its message beyond processing. Native access does not copy or box the message.</remarks>
public sealed class NativeMessage<TIncoming>
{
    internal NativeMessage() { }

    /// <summary>The untouched object or value supplied by the native receive callback.</summary>
    public TIncoming Message { get; internal set; } = default!;

    /// <summary>The resolved native settlement. Set only after durable failure handling or successful manual settlement.</summary>
    /// <remarks>Defaults to Complete. A native transport extension sets SettledByHandler after manual settlement succeeds.</remarks>
    public Disposition Disposition { get; set; } = Disposition.Complete;

    internal void Clear()
    {
        Message = default!;
        Disposition = Disposition.Complete;
    }
}
