using Relayer.Transports;
using System.Buffers;

namespace Relayer.Pipeline;

/// <summary>Decoded envelope metadata and native body storage supplied by a transport.</summary>
/// <remarks>The body and headers must remain valid until processing and context disposal finish.</remarks>
public readonly record struct ConsumeMessage
{
    /// <summary>Creates incoming metadata with delivery attempt one.</summary>
    public ConsumeMessage() { }

    /// <summary>The stable wire identity, decoded by the transport's envelope reader.</summary>
    public required MessageType MessageType { get; init; }

    /// <summary>The incoming message ID. It is preserved, never regenerated on receive.</summary>
    public required string MessageId { get; init; }

    /// <summary>The native body, without an intermediate copy.</summary>
    public required ReadOnlySequence<byte> Body { get; init; }

    /// <summary>A lazy native header view, when available. The pipeline never enumerates it.</summary>
    public MessageHeaders? Headers { get; init; }

    /// <summary>The delivery attempt, starting at one.</summary>
    public int Attempt { get; init; } = 1;
}
