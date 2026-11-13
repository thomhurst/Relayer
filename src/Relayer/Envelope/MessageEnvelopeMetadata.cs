using System.Diagnostics;

namespace Relayer.Transports;

/// <summary>Stable envelope metadata. Preserve this value across retries and outbox dispatches.</summary>
public readonly record struct MessageEnvelopeMetadata(Guid MessageId, DateTimeOffset Time, ActivityContext CreationContext)
{
    /// <summary>Captures a UUIDv7 and the current UTC time with the message's creation context.</summary>
    public static MessageEnvelopeMetadata Create(ActivityContext creationContext = default) =>
        new(Guid.CreateVersion7(), DateTimeOffset.UtcNow, creationContext);
}
