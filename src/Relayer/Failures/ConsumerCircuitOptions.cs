namespace Relayer;

/// <summary>Opt-in failure-rate protection for a consumer. Outcomes are counted after in-process retries.</summary>
public sealed class ConsumerCircuitOptions
{
    /// <summary>Fraction of failed deliveries that opens the circuit, greater than zero and at most one.</summary>
    public double FailureRatio { get; init; } = 0.5;
    /// <summary>Minimum completed deliveries in one sampling window.</summary>
    public int MinimumThroughput { get; init; } = 10;
    /// <summary>Fixed sampling window, at least one millisecond.</summary>
    public TimeSpan SamplingDuration { get; init; } = TimeSpan.FromSeconds(30);
    /// <summary>Time before admitting one recovery probe, at least one millisecond.</summary>
    public TimeSpan BreakDuration { get; init; } = TimeSpan.FromSeconds(5);
    internal void Validate() => ConsumerCircuit.Validate(FailureRatio, MinimumThroughput, SamplingDuration, BreakDuration);
}

// One adapter-owned serial delivery at a time. The policy itself remains concurrency safe.
internal sealed class ConsumerCircuitDelivery : IDisposable
{
    private readonly ConsumerCircuit _policy;
    private readonly EndpointTelemetry _telemetry;
    private bool _probe;
    private int _recordedState;
    internal bool Retained { get; private set; }
    internal int State => _policy.State;
    internal ConsumerCircuitDelivery(ConsumerCircuitOptions options, EndpointTelemetry telemetry, Action wake, TimeProvider time)
    {
        _telemetry = telemetry;
        _policy = new(options.FailureRatio, options.MinimumThroughput, options.SamplingDuration,
            options.BreakDuration, time, wake);
        telemetry.RecordCircuitState(0);
    }
    internal void RecordState()
    {
        var state = State;
        if (state == _recordedState) return;
        _recordedState = state;
        _telemetry.RecordCircuitState(state);
    }
    internal bool TryEnter()
    {
        Retained = false;
        var admitted = _policy.TryEnter(out _probe);
        RecordState();
        return admitted;
    }
    internal bool Complete(bool failed)
    {
        Retained = _policy.Complete(_probe, failed);
        RecordState();
        return Retained;
    }
    public void Dispose() => _policy.Dispose();
}
