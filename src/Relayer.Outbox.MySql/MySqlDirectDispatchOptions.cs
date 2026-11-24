namespace Relayer.Outbox.MySql;

/// <summary>Limits for commit-triggered, unordered MySQL outbox dispatch.</summary>
public sealed class MySqlDirectDispatchOptions
{
    private const int MaxClaimDurationDays = 1_000_000;
    private static readonly TimeSpan s_maxClaimDuration = TimeSpan.FromDays(MaxClaimDurationDays);

    /// <summary>Maximum queued commits. Overflow leaves durable rows pending and logs an error.</summary>
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

    internal static void ValidateClaimDuration(TimeSpan duration)
    {
        if (duration <= TimeSpan.Zero || duration > s_maxClaimDuration)
        {
            throw new ArgumentOutOfRangeException(nameof(ClaimDuration), duration,
                $"The duration must be positive and at most {MaxClaimDurationDays} days.");
        }
    }
}
