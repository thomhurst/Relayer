using System.Buffers;

namespace Relayer;

/// <summary>Reads an untouched native delivery without copying its body or decoding its headers.</summary>
public interface IMessageBinding<TIncoming> : IMessageHeaderReader<TIncoming>
{
    /// <summary>The borrowed native body, valid until receive processing completes.</summary>
    static abstract ReadOnlySequence<byte> GetBody(TIncoming message);

    /// <summary>The delivery count starting at one, or null when the broker does not count deliveries.</summary>
    static abstract int? GetDeliveryCount(TIncoming message);
}
