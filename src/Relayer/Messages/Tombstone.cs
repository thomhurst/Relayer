namespace Relayer;

/// <summary>Creates keyed deletions using the original message's stable wire identity and send route.</summary>
public static class Tombstone
{
    /// <summary>Creates a Kafka null-value record for the supplied, non-null key.</summary>
    public static Tombstone<T> For<T>(WireString key) => new(key);

    /// <summary>Creates a Kafka null-value record for a UTF-16 key.</summary>
    public static Tombstone<T> For<T>(string key) => new(new WireString(key));

    /// <summary>Creates a Kafka null-value record for a borrowed UTF-8 key.</summary>
    public static Tombstone<T> For<T>(ReadOnlyMemory<byte> key) => new(new WireString(key));
}

/// <summary>A native null-value record for <typeparamref name="T"/>. The key is borrowed during receive processing.</summary>
public readonly record struct Tombstone<T>(WireString Key) : ITombstone
{
    object ITombstone.Create(WireString key) => Tombstone.For<T>(key);
}

// Lets test observations reconstruct a registered typed deletion without reflection or JSON metadata.
internal interface ITombstone
{
    WireString Key { get; }
    object Create(WireString key);
}

internal sealed class TombstoneKeySelector<T> : IOrderingKeySelector<Tombstone<T>>
{
    public WireString? Select(Tombstone<T> message, System.Buffers.ArrayBufferWriter<byte> buffer) => message.Key;
}
