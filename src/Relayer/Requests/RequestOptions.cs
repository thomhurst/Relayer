namespace Relayer;

/// <summary>Bounds process-local request state for one request/response pair.</summary>
public sealed class RequestOptions
{
    /// <summary>The maximum number of overlapping requests. Exhaustion fails immediately instead of queuing unbounded work.</summary>
    public int MaxPendingRequests { get; init; } = 256;

    /// <summary>The clock used for request deadlines.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxPendingRequests, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaxPendingRequests, 65536);
        ArgumentNullException.ThrowIfNull(TimeProvider);
    }
}
