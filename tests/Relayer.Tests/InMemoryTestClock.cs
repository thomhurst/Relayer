using System.Threading.Channels;

namespace Relayer.Tests;

// Advances only on explicit test actions. Timer arming is observable, so no test sleeps for scheduler startup.
internal sealed class InMemoryTestClock : TimeProvider
{
    private readonly List<TestTimer> _timers = [];
    private readonly Channel<bool> _registered = Channel.CreateUnbounded<bool>();
    private int _created;
    private long _ticks = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).Ticks;
    public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);
    public override long GetTimestamp() => Interlocked.Read(ref _ticks);
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    internal async Task NextTimer() => await _registered.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    internal int TimersCreated => Volatile.Read(ref _created);

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new TestTimer(this, callback, state);
        Interlocked.Increment(ref _created);
        lock (_timers) _timers.Add(timer);
        timer.Change(dueTime, period);
        return timer;
    }

    internal void Advance(TimeSpan duration)
    {
        Interlocked.Add(ref _ticks, duration.Ticks);
        TestTimer[] timers;
        lock (_timers) timers = _timers.ToArray();
        foreach (var timer in timers) timer.Fire();
    }

    private sealed class TestTimer(InMemoryTestClock clock, TimerCallback callback, object? state) : ITimer
    {
        private long _due = long.MaxValue;
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            if (period != Timeout.InfiniteTimeSpan) throw new NotSupportedException("Tests use one-shot timers only.");
            Interlocked.Exchange(ref _due, dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : checked(clock.GetTimestamp() + dueTime.Ticks));
            if (dueTime != Timeout.InfiniteTimeSpan) clock._registered.Writer.TryWrite(true);
            return true;
        }
        internal void Fire()
        {
            var due = Interlocked.Read(ref _due);
            if (due <= clock.GetTimestamp() && Interlocked.CompareExchange(ref _due, long.MaxValue, due) == due) callback(state);
        }
        public void Dispose() => Interlocked.Exchange(ref _due, long.MaxValue);
        public ValueTask DisposeAsync() { Dispose(); return default; }
    }
}
