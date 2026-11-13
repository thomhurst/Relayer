namespace Relayer.Pipeline;

/// <summary>Contract failures are permanent; other failures enter the configured retry policy (D10).</summary>
public sealed class DefaultFailureClassifier : IFailureClassifier
{
    /// <inheritdoc />
    public FailureAction Classify(Exception exception, MessageContext context)
        => exception is UnknownMessageTypeException or MessageDeserializationException or MessageEnvelopeException
            ? FailureAction.DeadLetter : FailureAction.Retry;
}
