namespace Relayer;

/// <summary>Compares native keys without decoding or allocating, and releases accepted native deliveries.</summary>
public interface IPartitionMessageBinding<TIncoming> : IMessageBinding<TIncoming>
{
    /// <summary>Whether two deliveries share an ordering key. Define native null-key behavior explicitly.</summary>
    static abstract bool KeysEqual(TIncoming first, TIncoming second);

    /// <summary>Releases a delivery's borrowed storage exactly once. Must not throw or reenter the scheduler.</summary>
    /// <remarks>Called after processing and checkpoint capture, or after discarded queued work. Never called for rejected admission.</remarks>
    static virtual void Release(TIncoming message) { }
}
