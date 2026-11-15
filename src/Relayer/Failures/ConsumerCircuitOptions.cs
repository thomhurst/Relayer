using Relayer.Transports;
using Relayer.Pipeline;
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
