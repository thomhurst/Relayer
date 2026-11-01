namespace Relayer;

/// <summary>The consumer has no generated handler for an incoming wire identity.</summary>
public sealed class UnknownMessageTypeException(MessageType messageType)
    : Exception($"No handler is configured for message type '{messageType}'. Register its stable identity and generated handler on this consumer.")
{
    /// <summary>The unrecognized wire identity.</summary>
    public MessageType MessageType { get; } = messageType;
}

/// <summary>An incoming body could not be decoded as its declared message contract.</summary>
public sealed class MessageDeserializationException(MessageType messageType, Exception innerException)
    : Exception($"Cannot deserialize message type '{messageType}'. Check its payload and registered serializer.", innerException)
{
    /// <summary>The incoming wire identity.</summary>
    public MessageType MessageType { get; } = messageType;
}

/// <summary>Contract failures are permanent; other failures enter the configured retry policy (D10).</summary>
public sealed class DefaultFailureClassifier : IFailureClassifier
{
    /// <inheritdoc />
    public FailureAction Classify(Exception exception, MessageContext context)
        => exception is UnknownMessageTypeException or MessageDeserializationException
            ? FailureAction.DeadLetter : FailureAction.Retry;
}
