using System.Buffers;

namespace Relayer.Serialization;

/// <summary>Serializes message bodies. Buffer-based to avoid intermediate allocations (docs/decisions.md D8).</summary>
public interface IMessageSerializer
{
    /// <summary>The content type written to the native message, e.g. <c>application/json</c>.</summary>
    string ContentType { get; }

    /// <summary>Writes <paramref name="message"/> to <paramref name="destination"/>.</summary>
    void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination);

    /// <summary>Reads a <typeparamref name="TMessage"/> from <paramref name="source"/>.</summary>
    TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source);
}
