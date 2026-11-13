using Relayer.Transports;

namespace Relayer;

/// <summary>Retry and redelivery delays, composed once per consumer. Each entry permits one additional attempt.</summary>
public sealed class FailureOptions(
    TimeSpan[]? RetryDelays = null,
    TimeSpan[]? RedeliveryDelays = null,
    double Jitter = 0.2,
    string? DeadLetterEndpoint = null,
    TimeSpan? CopyTimeout = null)
{
    /// <summary>No in-process retries or durable redeliveries. Retryable failures dead-letter immediately.</summary>
    public static FailureOptions None => new() { RetryDelays = [] };

    /// <summary>In-process retry backoffs. Null selects 100 ms, 500 ms and 1 s before jitter. Set empty to disable.</summary>
    /// <remarks>Null keeps configuration binding from appending configured delays to the default sequence.</remarks>
    public TimeSpan[]? RetryDelays { get; init; } = RetryDelays;
    /// <summary>Backoffs between durable redeliveries. Empty by default.</summary>
    public TimeSpan[] RedeliveryDelays
    {
        get => _redeliveryDelays;
        init => _redeliveryDelays = value ?? [];
    }
    private readonly TimeSpan[] _redeliveryDelays = RedeliveryDelays ?? [];
    /// <summary>Randomly shorten nonzero delays by up to this fraction. Between zero and one.</summary>
    public double Jitter { get; init; } = Jitter;
    /// <summary>Explicit dead-letter destination on the consumer's transport when native dead-lettering is unavailable.</summary>
    public string? DeadLetterEndpoint { get; init; } = DeadLetterEndpoint;
    /// <summary>Timeout for a scheduled or dead-letter copy. Null selects 30 seconds. Native operations observe receive cancellation.</summary>
    public TimeSpan? CopyTimeout { get; init; } = CopyTimeout;
}

/// <summary>Creates a borrowed copy of the original body, all headers, and native ordering key.</summary>
/// <remarks>
/// Called only on failure. Preserve malformed envelopes, original identity and creation context as-is.
/// The pipeline replaces Destination, DeliverAt and its own error headers. Storage must survive until the returned send completes.
/// Use the transport's NativeMessage feature to access data not exposed by the lazy context.
/// </remarks>
public delegate OutgoingMessage FailureMessageCopyDelegate(MessageContext context);

/// <summary>An available executor that commits a scheduled copy to durable storage independently of the failed attempt.</summary>
public interface IFailureScheduler
{
    /// <summary>Persist the borrowed message before returning. A successful return permits completing the original.</summary>
    ValueTask ScheduleAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken);
}
