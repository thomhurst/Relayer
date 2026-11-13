namespace Relayer.Transports;

/// <summary>Native operations used by the single-owner partition scheduler.</summary>
/// <remarks>
/// Calls run on the transport's pump, never on handler continuations. Do not reenter the scheduler.
/// Commit must capture the opaque checkpoint before returning; the transport owns batching, flush,
/// native ownership fencing and retries. A throwing Commit is retried by the next pump.
/// </remarks>
public interface IPartitionSource<TPartition, TPosition>
{
    /// <summary>Captures the last durably completed checkpoint, with native position semantics unchanged.</summary>
    void Commit(TPartition partition, TPosition position);
    /// <summary>Stops native fetching for this assignment.</summary>
    void Pause(TPartition partition);
    /// <summary>Restarts native fetching for this assignment.</summary>
    void Resume(TPartition partition);
}
