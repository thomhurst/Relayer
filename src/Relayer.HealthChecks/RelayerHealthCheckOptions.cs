namespace Relayer.HealthChecks;

/// <summary>Thresholds shared by Relayer health checks. Values at a threshold fail that threshold.</summary>
public sealed class RelayerHealthCheckOptions
{
    /// <summary>Maximum time an active consumer can go without progress. Defaults to five minutes.</summary>
    public TimeSpan ConsumerStallTimeout { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Pending dispatch count that degrades health. Defaults to 1,000 rows.</summary>
    public long OutboxDegradedThreshold { get; init; } = 1_000;

    /// <summary>Pending dispatch count that fails health. Defaults to 10,000 rows.</summary>
    public long OutboxUnhealthyThreshold { get; init; } = 10_000;

    /// <summary>Expected outbox names. Every name requires a keyed IOutboxHealthProvider, even before an adapter exists.</summary>
    public IReadOnlyList<string> OutboxNames { get; init; } = [];
}
