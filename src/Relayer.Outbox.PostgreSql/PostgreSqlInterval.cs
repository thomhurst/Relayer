using NpgsqlTypes;

namespace Relayer.Outbox.PostgreSql;

internal static class PostgreSqlInterval
{
    internal static TimeSpan RoundLeaseUp(TimeSpan duration)
    {
        // Lease options are capped at one hour, so Npgsql encodes the rounded value without days.
        return TimeSpan.FromTicks(RoundMicrosecondsUp(duration.Ticks) * TimeSpan.TicksPerMicrosecond);
    }

    internal static NpgsqlInterval ElapsedDuration(TimeSpan duration)
    {
        // PostgreSQL adds interval days in the session time zone. Use the time field for elapsed time.
        return new(0, 0, RoundMicrosecondsUp(duration.Ticks));
    }

    internal static NpgsqlInterval CalendarDuration(TimeSpan duration)
    {
        // Preserve calendar-day arithmetic, even when the rounded time component reaches 24 hours.
        long ticksInDay = duration.Ticks % TimeSpan.TicksPerDay;
        return new(0, duration.Days, RoundMicrosecondsUp(ticksInDay));
    }

    private static long RoundMicrosecondsUp(long ticks)
    {
        var (microseconds, remainder) = long.DivRem(ticks, TimeSpan.TicksPerMicrosecond);
        return remainder > 0 ? microseconds + 1 : microseconds;
    }
}
