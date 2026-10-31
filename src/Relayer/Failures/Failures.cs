namespace Relayer;

/// <summary>What to do with a message whose handler failed.</summary>
public enum FailureAction
{
    /// <summary>Retry according to the configured retry and redelivery policy.</summary>
    Retry,

    /// <summary>Send straight to the dead-letter destination.</summary>
    DeadLetter,

    /// <summary>Acknowledge and drop the message. Use sparingly.</summary>
    Discard,
}

/// <summary>Classifies exceptions into failure actions (docs/decisions.md D10).</summary>
public interface IFailureClassifier
{
    /// <summary>Classifies <paramref name="exception"/> raised while handling the message in <paramref name="context"/>.</summary>
    FailureAction Classify(Exception exception, MessageContext context);
}

/// <summary>Lets a handler settle a message manually (docs/decisions.md D11).</summary>
public interface ISettlementFeature
{
    /// <summary>Completes the message.</summary>
    void Complete();

    /// <summary>Requests redelivery after <paramref name="delay"/>.</summary>
    void Retry(TimeSpan delay);

    /// <summary>Dead-letters the message with a reason.</summary>
    void DeadLetter(string reason);
}
