namespace Relayer.Tests;

public sealed class SystemTimerDelayTests
{
    [Test]
    [Arguments(TimeSpan.TicksPerMillisecond, 1L)]
    [Arguments(TimeSpan.TicksPerMillisecond + 1, 2L)]
    [Arguments(19_000L, 2L)]
    [Arguments(10_009_000L, 1_001L)]
    [Arguments(TimeSpan.TicksPerHour, 3_600_000L)]
    [Arguments((uint.MaxValue - 1L) * TimeSpan.TicksPerMillisecond - 1, uint.MaxValue - 1L)]
    [Arguments((uint.MaxValue - 1L) * TimeSpan.TicksPerMillisecond, uint.MaxValue - 1L)]
    public async Task System_timer_delay_rounds_up_to_whole_milliseconds(long ticks, long milliseconds)
    {
        var delay = SystemTimerDelay.RoundUp(TimeSpan.FromTicks(ticks));

        await Assert.That(delay).IsEqualTo(TimeSpan.FromMilliseconds(milliseconds));
    }
}
