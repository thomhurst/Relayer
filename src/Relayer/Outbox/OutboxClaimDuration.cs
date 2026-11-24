namespace Relayer.Outbox;

internal static class OutboxClaimDuration
{
    private const int MaxClaimDurationDays = 1_000_000;
    // Leave millennia of headroom when the dispatch worker adds the duration to the UTC clock.
    private static readonly TimeSpan s_maxClaimDuration = TimeSpan.FromDays(MaxClaimDurationDays);

    internal static void Validate(TimeSpan duration, string paramName, bool requireMicrosecondPrecision = false)
    {
        long minimumTicks = requireMicrosecondPrecision ? TimeSpan.TicksPerMicrosecond : 1;

        if (duration.Ticks < minimumTicks || duration > s_maxClaimDuration)
        {
            string minimum = requireMicrosecondPrecision ? "at least 1 microsecond" : "positive";
            throw new ArgumentOutOfRangeException(paramName, duration,
                $"The duration must be {minimum} and at most {MaxClaimDurationDays} days.");
        }
    }
}
