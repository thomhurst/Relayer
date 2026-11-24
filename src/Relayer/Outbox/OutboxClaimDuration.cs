namespace Relayer.Outbox;

internal static class OutboxClaimDuration
{
    private const int MaxClaimDurationDays = 1_000_000;
    // Leave millennia of headroom when the dispatch worker adds the duration to the UTC clock.
    private static readonly TimeSpan s_maxClaimDuration = TimeSpan.FromDays(MaxClaimDurationDays);

    internal static void Validate(TimeSpan duration, string paramName)
    {
        if (duration <= TimeSpan.Zero || duration > s_maxClaimDuration)
        {
            throw new ArgumentOutOfRangeException(paramName, duration,
                $"The duration must be positive and at most {MaxClaimDurationDays} days.");
        }
    }
}
