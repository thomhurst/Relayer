namespace Relayer;

// Transport-independent policy only. Native admission, settlement and lifecycle remain the adapter's job.
// Count completed logical deliveries, after retries; do not count individual retry attempts.
internal sealed class ConsumerCircuit : IDisposable
{
    private const long CountMask = (1L << 30) - 1;
    private const int FailureShift = 30;
    private const int StateShift = 60;
    private const long Open = 1L << StateShift;
    private const long ProbeAvailable = 2L << StateShift;
    private const long ProbeRunning = 3L << StateShift;
    private const long StateMask = 3L << StateShift;
    private readonly double _failureRatio;
    private readonly int _minimumThroughput;
    private readonly TimeSpan _breakDuration;
    private readonly Action? _probeAvailable;
    private readonly ITimer _sampling;
    private readonly ITimer _recovery;
    private long _state;
    private int _disposed;

    internal ConsumerCircuit(double failureRatio, int minimumThroughput, TimeSpan samplingDuration,
        TimeSpan breakDuration, TimeProvider time, Action? probeAvailable = null)
    {
        ArgumentNullException.ThrowIfNull(time);
        Validate(failureRatio, minimumThroughput, samplingDuration, breakDuration);
        _failureRatio = failureRatio;
        _minimumThroughput = minimumThroughput;
        _breakDuration = breakDuration;
        _probeAvailable = probeAvailable;
        // Construct disabled: callbacks must never observe partially initialized timer fields.
        _sampling = time.CreateTimer(static state => ((ConsumerCircuit)state!).ResetWindow(), this,
            Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        try
        {
            _recovery = time.CreateTimer(static state => ((ConsumerCircuit)state!).OfferProbe(), this,
                Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            _sampling.Change(samplingDuration, samplingDuration);
        }
        catch { _recovery?.Dispose(); _sampling.Dispose(); throw; }
    }

    // Values match EndpointTelemetry.RecordCircuitState. Offering a probe does not resume a native consumer.
    internal int State => (Volatile.Read(ref _state) & StateMask) switch
    {
        Open => 1,
        ProbeAvailable or ProbeRunning => 2,
        _ => 0
    };

    // The caller must complete each admitted attempt exactly once. A bool token distinguishes an admitted
    // probe from work already in flight when the circuit opens. Rejected admission must not run a handler.
    // An aborted probe must complete as failed (or dispose the policy during shutdown).
    internal bool TryEnter(out bool probe)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        probe = false;
        while (true)
        {
            var state = Volatile.Read(ref _state);
            if ((state & StateMask) == 0) return true;
            if (state != ProbeAvailable) return false;
            if (Interlocked.CompareExchange(ref _state, ProbeRunning, state) != state) continue;
            probe = true;
            return true;
        }
    }

    // Returns true when this outcome opens/reopens the circuit. It is a policy decision, never a native
    // pause acknowledgement. The adapter must retain a failing delivery before applying native control.
    internal bool Complete(bool probe, bool failed)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (probe)
        {
            if (Interlocked.CompareExchange(ref _state, failed ? Open : 0, ProbeRunning) != ProbeRunning)
                throw new InvalidOperationException("No circuit probe is in flight.");
            if (failed) ScheduleRecovery();
            return failed;
        }
        while (true)
        {
            var state = Volatile.Read(ref _state);
            if ((state & StateMask) != 0) return false;
            var total = state & CountMask;
            var failures = (state >> FailureShift) & CountMask;
            // A window also ends at the bounded counter capacity (over one billion outcomes).
            if (total == CountMask) { total = 0; failures = 0; }
            total++;
            if (failed) failures++;
            var open = total >= _minimumThroughput && failures / (double)total >= _failureRatio;
            var next = open ? Open : total | (failures << FailureShift);
            if (Interlocked.CompareExchange(ref _state, next, state) != state) continue;
            if (open) ScheduleRecovery();
            return open;
        }
    }

    private void ResetWindow()
    {
        while (true)
        {
            var state = Volatile.Read(ref _state);
            if ((state & StateMask) != 0) return;
            if (Interlocked.CompareExchange(ref _state, 0, state) == state) return;
        }
    }

    // The optional callback is an adapter-owned, nonthrowing control-lane signal, never native lifecycle
    // work. In particular, it must not stop/start a Service Bus processor on this timer thread.
    private void OfferProbe()
    {
        if (Interlocked.CompareExchange(ref _state, ProbeAvailable, Open) == Open &&
            Volatile.Read(ref _disposed) == 0) _probeAvailable?.Invoke();
    }

    private void ScheduleRecovery()
    {
        // Disposal may race a native shutdown. Timer disposal prevents later callbacks; a queued callback
        // can only offer admission on this abandoned policy, never restart native processing.
        try { _recovery.Change(_breakDuration, Timeout.InfiniteTimeSpan); }
        catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) != 0) { }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _sampling.Dispose();
        _recovery.Dispose();
    }

    internal static void Validate(double failureRatio, int minimumThroughput, TimeSpan samplingDuration, TimeSpan breakDuration)
    {
        if (!double.IsFinite(failureRatio) || failureRatio <= 0 || failureRatio > 1)
            throw new ArgumentOutOfRangeException(nameof(failureRatio));
        if (minimumThroughput < 1 || minimumThroughput > CountMask)
            throw new ArgumentOutOfRangeException(nameof(minimumThroughput));
        ValidateDuration(samplingDuration, nameof(samplingDuration));
        ValidateDuration(breakDuration, nameof(breakDuration));
    }

    private static void ValidateDuration(TimeSpan duration, string name)
    {
        if (duration < TimeSpan.FromMilliseconds(1) || duration.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(name, "Duration must be at least one millisecond and fit a timer.");
    }
}
