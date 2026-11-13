namespace Relayer.Transports;

/// <summary>Native send configuration access for transport and persistence implementations.</summary>
public static class NativeSendOptionsExtensions
{
    /// <summary>Whether callbacks are present and cannot be persisted in an outbox.</summary>
    public static bool HasNativeConfigurators(this SendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.HasNativeConfigurators;
    }

    /// <summary>Applies callbacks in registration order using the transport's native outgoing type.</summary>
    /// <remarks>No wrapping closure, reflection, or dynamic invocation is used.</remarks>
    public static void ApplyNative<TNative>(this SendOptions options, TNative nativeMessage) where TNative : class
    {
        ArgumentNullException.ThrowIfNull(options);
        options.ApplyNativeCore(nativeMessage);
    }
}
