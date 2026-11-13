namespace Relayer;

/// <summary>Retry and redelivery delays, composed once per consumer. Each entry permits one additional attempt.</summary>
public sealed class FailureOptions(
    TimeSpan[]? RetryDelays = null,
    TimeSpan[]? RedeliveryDelays = null,
    double Jitter = 0.2,
    string? DeadLetterEndpoint = null,
    TimeSpan? CopyTimeout = null)
{
    /// <summary>Creates the default policy for direct construction and Options-pattern binding.</summary>
    public FailureOptions() : this(null, null, 0.2, null, null) { }

    /// <summary>No in-process retries or durable redeliveries. Retryable failures dead-letter immediately.</summary>
    public static FailureOptions None => new() { RetryDelays = [] };

    /// <summary>In-process retry backoffs. Null selects 100 ms, 500 ms and 1 s before jitter. Set empty to disable.</summary>
    /// <remarks>Null keeps configuration binding from appending configured delays to the default sequence.</remarks>
    public TimeSpan[]? RetryDelays { get; set; } = RetryDelays;
    /// <summary>Backoffs between durable redeliveries. Empty by default.</summary>
    public TimeSpan[] RedeliveryDelays
    {
        get => _redeliveryDelays;
        set => _redeliveryDelays = value ?? [];
    }
    private TimeSpan[] _redeliveryDelays = RedeliveryDelays ?? [];
    /// <summary>Randomly shorten nonzero delays by up to this fraction. Between zero and one.</summary>
    public double Jitter { get; set; } = Jitter;
    /// <summary>Explicit dead-letter destination on the consumer's transport when native dead-lettering is unavailable.</summary>
    public string? DeadLetterEndpoint { get; set; } = DeadLetterEndpoint;
    /// <summary>Timeout for a scheduled or dead-letter copy. Null selects 30 seconds. Native operations observe receive cancellation.</summary>
    public TimeSpan? CopyTimeout { get; set; } = CopyTimeout;
}
