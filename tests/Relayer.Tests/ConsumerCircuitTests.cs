using Relayer.Pipeline;
namespace Relayer.Tests;

public class ConsumerCircuitTests
{
    [Test]
    public async Task Minimum_throughput_ratio_windows_and_probe_outcomes()
    {
        var time = new Clock();
        using var circuit = new ConsumerCircuit(.5, 4, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), time);
        await Assert.That(circuit.TryEnter(out var initialProbe)).IsTrue();
        await Assert.That(initialProbe).IsFalse();
        await Assert.That(circuit.Complete(false, true)).IsFalse();
        await Assert.That(circuit.Complete(false, true)).IsFalse();
        time.Sampling.Fire(); // Expired failures do not contribute to the next fixed window.
        circuit.Complete(false, false);
        circuit.Complete(false, false);
        circuit.Complete(false, true);
        await Assert.That(circuit.Complete(false, true)).IsTrue();
        await Assert.That(circuit.State).IsEqualTo(1);
        await Assert.That(circuit.TryEnter(out _)).IsFalse();
        // Outcomes already in flight and sampling ticks cannot close an open circuit.
        circuit.Complete(false, false);
        time.Sampling.Fire();
        await Assert.That(circuit.State).IsEqualTo(1);
        await Assert.That(time.Recovery.Due).IsEqualTo(TimeSpan.FromSeconds(5));
        time.Recovery.Fire();
        await Assert.That(circuit.State).IsEqualTo(2);
        await Assert.That(circuit.TryEnter(out var probe)).IsTrue();
        await Assert.That(probe).IsTrue();
        await Assert.That(circuit.TryEnter(out _)).IsFalse();
        circuit.Complete(false, false); // A late original attempt cannot complete the probe.
        await Assert.That(circuit.State).IsEqualTo(2);
        await Assert.That(circuit.Complete(probe, true)).IsTrue();
        await Assert.That(circuit.State).IsEqualTo(1);
        time.Recovery.Fire();
        await Assert.That(circuit.TryEnter(out probe)).IsTrue();
        await Assert.That(circuit.Complete(probe, false)).IsFalse();
        await Assert.That(circuit.State).IsEqualTo(0);
        // A successful probe starts with empty counters.
        await Assert.That(circuit.Complete(false, true)).IsFalse();
    }

    [Test]
    public async Task A_success_can_reach_minimum_throughput_with_a_high_failure_rate()
    {
        var time = new Clock();
        using var circuit = new ConsumerCircuit(.75, 4, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), time);
        for (var i = 0; i < 3; i++) circuit.Complete(false, true);
        await Assert.That(circuit.Complete(false, false)).IsTrue();
        await Assert.That(circuit.State).IsEqualTo(1);
    }

    [Test]
    public async Task Concurrent_outcomes_are_counted_and_only_one_probe_is_admitted()
    {
        var time = new Clock();
        using var circuit = new ConsumerCircuit(1, 1000, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), time);
        var opened = 0;
        Parallel.For(0, 1000, _ => { if (circuit.Complete(false, true)) Interlocked.Increment(ref opened); });
        await Assert.That(opened).IsEqualTo(1);
        await Assert.That(circuit.State).IsEqualTo(1);
        time.Recovery.Fire();
        var admitted = 0;
        Parallel.For(0, 1000, _ => { if (circuit.TryEnter(out var probe) && probe) Interlocked.Increment(ref admitted); });
        await Assert.That(admitted).IsEqualTo(1);
        await Assert.That(circuit.State).IsEqualTo(2);
    }

    [Test]
    public async Task Closed_admission_and_outcomes_allocate_nothing_after_construction()
    {
        using var circuit = new ConsumerCircuit(1, 10, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), new Clock());
        for (var i = 0; i < 10000; i++) { circuit.TryEnter(out var probe); circuit.Complete(probe, false); }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10000; i++) { circuit.TryEnter(out var probe); circuit.Complete(probe, false); }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    [Test]
    public async Task Sampling_reset_does_not_interrupt_probe_and_dispose_stops_timers()
    {
        var time = new Clock();
        var signals = 0;
        var circuit = new ConsumerCircuit(1, 1, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), time, () => signals++);
        circuit.Complete(false, true);
        time.Recovery.Fire();
        time.Recovery.Fire(); // A stale timer tick must not offer a second probe.
        await Assert.That(signals).IsEqualTo(1);
        circuit.TryEnter(out var probe);
        time.Sampling.Fire();
        await Assert.That(circuit.TryEnter(out _)).IsFalse();
        circuit.Complete(probe, false);
        circuit.Dispose();
        circuit.Dispose();
        await Assert.That(time.Sampling.Disposed).IsTrue();
        await Assert.That(time.Recovery.Disposed).IsTrue();
        await Assert.That(() => circuit.TryEnter(out _)).Throws<ObjectDisposedException>();
    }

    [Test]
    [Arguments(double.NaN, 1, 1000, 1000)]
    [Arguments(0, 1, 1000, 1000)]
    [Arguments(1.1, 1, 1000, 1000)]
    [Arguments(.5, 0, 1000, 1000)]
    [Arguments(.5, 1, 0, 1000)]
    [Arguments(.5, 1, 1000, -1)]
    public async Task Invalid_policy_is_rejected(double ratio, int minimum, int samplingMs, int breakMs)
    {
        await Assert.That(() => new ConsumerCircuit(ratio, minimum, TimeSpan.FromMilliseconds(samplingMs),
            TimeSpan.FromMilliseconds(breakMs), new Clock())).Throws<ArgumentOutOfRangeException>();
    }

    internal sealed class Clock : TimeProvider
    {
        private readonly List<Timer> _timers = [];
        internal Timer Sampling => _timers[0];
        internal Timer Recovery => _timers[1];
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new Timer(callback, state);
            timer.Change(dueTime, period);
            _timers.Add(timer);
            return timer;
        }
    }

    internal sealed class Timer(TimerCallback callback, object? state) : ITimer
    {
        internal TimeSpan Due { get; private set; }
        internal bool Disposed { get; private set; }
        internal void Fire() => callback(state);
        public bool Change(TimeSpan dueTime, TimeSpan period) { Due = dueTime; return !Disposed; }
        public void Dispose() => Disposed = true;
        public ValueTask DisposeAsync() { Dispose(); return default; }
    }
}
