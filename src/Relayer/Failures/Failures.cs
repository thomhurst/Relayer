using Relayer.Transports;
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

/// <summary>Requests a settlement outcome after successful handling (docs/decisions.md D11).</summary>
/// <remarks>
/// Get from MessageContext.Features. One request is allowed per attempt. A handler or middleware exception
/// overrides the request; cancellation, scope disposal and durable-write failures prevent settlement.
/// Transaction boundaries may request completion before commit; further requests during scope disposal then throw.
/// Do not retain this pooled feature beyond processing. Native settlement is deferred until durable work succeeds.
/// </remarks>
public interface ISettlementFeature
{
    /// <summary>The requested outcome, or null when handling has not requested settlement.</summary>
    Disposition? RequestedDisposition { get; }

    /// <summary>Requests completion after successful handling and durable sends.</summary>
    void Complete();

    /// <summary>Requests redelivery after the exact non-negative <paramref name="delay"/>, without in-process retries or jitter.</summary>
    void Retry(TimeSpan delay);

    /// <summary>Dead-letters the message with a reason.</summary>
    void DeadLetter(string reason);
}
