using Relayer.Transports;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Relayer.HealthChecks;

/// <summary>A native connectivity probe, registered as a keyed service under the transport name.</summary>
/// <remarks>Use a read-only native client operation. Do not send or consume a test message.</remarks>
public interface ITransportHealthProbe
{
    /// <summary>Reports native connectivity, preserving degraded/unhealthy status and useful failure details.</summary>
    ValueTask<HealthCheckResult> CheckAsync(CancellationToken cancellationToken);
}

/// <summary>Provides consumer snapshots, registered as a keyed service under the transport name.</summary>
public interface IConsumerHealthProvider
{
    /// <summary>Reads the state of this exact declaration, including all its workers.</summary>
    /// <remarks>
    /// Idle means every worker is waiting for messages. For active workers, return the oldest last-progress
    /// time (or activation time before first progress), so one stalled worker cannot hide behind another.
    /// Return null when state is unavailable. Never infer idle from the absence of progress.
    /// </remarks>
    ValueTask<ConsumerHealthState?> GetStateAsync(IConsumerDefinition consumer, CancellationToken cancellationToken);
}

/// <summary>A consumer snapshot. LastProgressAt includes activation when no progress has occurred yet.</summary>
public readonly record struct ConsumerHealthState(bool IsIdle, DateTimeOffset LastProgressAt);

/// <summary>Provides backlog counts, registered as a keyed service under the configured outbox name.</summary>
public interface IOutboxHealthProvider
{
    /// <summary>Returns pending rows eligible for dispatch, excluding future scheduled rows; null means unavailable.</summary>
    ValueTask<long?> GetPendingCountAsync(CancellationToken cancellationToken);
}
