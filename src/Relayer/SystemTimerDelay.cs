namespace Relayer;

internal static class SystemTimerDelay
{
    internal static TimeSpan RoundUp(TimeSpan delay)
    {
        // The runtime timer truncates fractional milliseconds.
        return TimeSpan.FromMilliseconds(Math.Ceiling(delay.TotalMilliseconds));
    }
}
