namespace Relayer;

/// <summary>Writes headers using a transport's native binary-mode binding.</summary>
public interface IMessageHeaderWriter<TOutgoing>
{
    /// <summary>Sets a header, replacing an existing value with the same name.</summary>
    /// <remarks>The implementation must consume or copy the span before returning.</remarks>
    static abstract void SetHeader(TOutgoing message, string name, ReadOnlySpan<char> value);

    /// <summary>Removes a header, if present.</summary>
    static abstract void RemoveHeader(TOutgoing message, string name);
}
