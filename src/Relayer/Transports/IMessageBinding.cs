using System.Buffers;

namespace Relayer;

/// <summary>Reads an untouched native delivery without copying its body or decoding its headers.</summary>
public interface IMessageBinding<TIncoming> : IMessageHeaderReader<TIncoming>
{
    /// <summary>The borrowed native body, valid until receive processing completes.</summary>
    static abstract ReadOnlySequence<byte> GetBody(TIncoming message);

    /// <summary>Whether the native record has a null value. Never infer this from application headers.</summary>
    static virtual bool IsTombstone(TIncoming message) => false;

    /// <summary>The borrowed native key for a tombstone, or null when absent.</summary>
    static virtual WireString? GetTombstoneKey(TIncoming message) => null;

    /// <summary>The delivery count starting at one, or null when the broker does not count deliveries.</summary>
    static abstract int? GetDeliveryCount(TIncoming message);

    /// <summary>The native partition ID for sampling, or null when unpartitioned. Return a cached string without per-delivery formatting.</summary>
    /// <remarks>Called only when the Relayer source has a listener.</remarks>
    static virtual string? GetPartitionId(TIncoming message) => null;

    /// <summary>Adds native message attributes such as key, offset, tombstone or delivery count to a recorded operation.</summary>
    /// <remarks>Called only when all activity data is requested. Do not retain the borrowed native delivery.</remarks>
    static virtual void EnrichActivity(System.Diagnostics.Activity activity, TIncoming message) { }

    /// <summary>Consumes a native settlement error's logging marker, so the pipeline does not log that same error again.</summary>
    /// <remarks>Called only on failure. Return true only when this delivery already logged this exact exception. Clear markers before reusing native state.</remarks>
    static virtual bool TryConsumeLoggedSettlementError(TIncoming message, Exception error) => false;
}
