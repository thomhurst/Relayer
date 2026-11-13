using Relayer.Transports;
namespace Relayer;

/// <summary>Per-message context passed to handlers and consume middleware.</summary>
public abstract class MessageContext
{
    /// <summary>The stable message ID (UUIDv7), carried in <c>ce_id</c>.</summary>
    public abstract string MessageId { get; }

    /// <summary>The wire identity of the message.</summary>
    public abstract MessageType MessageType { get; }

    /// <summary>The message headers, read lazily from the native message.</summary>
    public abstract MessageHeaders Headers { get; }

    /// <summary>The producer's outbox insert sequence, read lazily; null when absent or malformed.</summary>
    /// <remarks>Compare only for one ordering key, outbox table and tenant. Concurrent transactions have no defined order.</remarks>
    public long? OutboxSequence
    {
        get
        {
            if (!Headers.TryGetValue(OutboxHeaders.Sequence, out var value)) return null;
            var parsed = value.Text is { } text
                ? long.TryParse(text.AsSpan(), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var sequence)
                : long.TryParse(value.Utf8.Span, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out sequence);
            return parsed && sequence > 0 ? sequence : null;
        }
    }

    /// <summary>The delivery attempt, starting at 1.</summary>
    public abstract int Attempt { get; }

    /// <summary>The DI scope created for this message.</summary>
    public abstract IServiceProvider Services { get; }

    /// <summary>Typed features, including transport-native objects.</summary>
    public abstract MessageFeatures Features { get; }

    /// <summary>Signalled when the consumer is stopping or the message's lock is lost.</summary>
    public abstract CancellationToken CancellationToken { get; }
}
