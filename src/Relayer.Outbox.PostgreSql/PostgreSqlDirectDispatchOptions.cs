namespace Relayer.Outbox.PostgreSql;

/// <summary>Limits for commit-triggered, unordered PostgreSQL outbox dispatch.</summary>
public sealed class PostgreSqlDirectDispatchOptions
{
    /// <summary>Maximum queued commits. Overflow leaves durable rows pending and logs an error.</summary>
    /// <remarks>Also bounds the number of unordered local rows the worker can defer while consumers are full.</remarks>
    public int QueueCapacity
    {
        get; init;
    } = 256;

    /// <summary>Maximum messages claimed and sent together, also bounded by the native endpoint limit.</summary>
    public int BatchSize
    {
        get; init;
    } = 100;

    /// <summary>Lifetime of a fenced claim, greater than zero and at most 1,000,000 days. A send that outlives it can be delivered again.</summary>
    public TimeSpan ClaimDuration
    {
        get; init;
    } = TimeSpan.FromSeconds(30);

    internal static void ValidateClaimDurationResolution(TimeSpan duration)
    {
        if (duration.Ticks is > 0 and < TimeSpan.TicksPerMicrosecond)
        {
            throw new ArgumentOutOfRangeException(nameof(ClaimDuration), duration,
                "PostgreSQL claim durations must be at least 1 microsecond.");
        }
    }
}
