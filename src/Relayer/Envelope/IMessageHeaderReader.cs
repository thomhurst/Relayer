namespace Relayer;

/// <summary>Reads one header directly from a native incoming message.</summary>
/// <remarks>Implementations must not eagerly materialize or decode the native header collection.</remarks>
public interface IMessageHeaderReader<TIncoming>
{
    /// <summary>Finds a native header without converting its value to a string.</summary>
    static abstract bool TryGetHeader(TIncoming message, string name, out WireString value);
}
