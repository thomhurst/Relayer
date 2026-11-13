using Relayer.Transports;
namespace Relayer;

/// <summary>A lazy, non-enumerating view of native headers. Values retain their native encoding.</summary>
/// <remarks>The view and returned memory are borrowed for the duration of message processing.</remarks>
public abstract class MessageHeaders
{
    /// <summary>An immutable empty header view.</summary>
    public static MessageHeaders Empty { get; } = new EmptyHeaders();

    /// <summary>Reads one named header without decoding its value.</summary>
    public abstract bool TryGetValue(string name, out WireString value);

    private sealed class EmptyHeaders : MessageHeaders
    {
        public override bool TryGetValue(string name, out WireString value)
        {
            value = default;
            return false;
        }
    }
}
