namespace Relayer.Transports;

/// <summary>The durable read and claim operations used by an outbox dispatcher.</summary>
/// <remarks>
/// Register a scoped implementation as a keyed service under an explicit store name. Transactional writes
/// belong to the native database integration and <see cref="IOutboxEnlistment"/>, not this interface.
/// All mutations are atomic and durable before completion. Observe cancellation and propagate database errors;
/// a cancelled or faulted operation may have committed, so callers must tolerate at-least-once delivery.
/// Instances need not support concurrent calls. Ordered dispatch additionally requires the lane lease protocol;
/// reading or claiming a row alone does not establish ordering. Dispatch workers and lane leases are separate integrations.
/// </remarks>
public interface IOutboxStore
{
    /// <summary>Reads eligible rows from one lane (null selects unordered rows), in ascending UUIDv7 ID order.</summary>
    /// <remarks>
    /// Return at most destination.Length rows and only IDs strictly after request.AfterId when supplied.
    /// Eligibility means pending, not parked or dead, due (DeliverAt is null or at most request.Now),
    /// and without a live claim (claim expiry at most request.Now is expired). Reading does not claim rows.
    /// A zero-length destination returns zero. Providers must reject a negative lane.
    /// Row payload memory remains valid until the next read on this instance or its disposal. Finish all sends
    /// before that next read; callers own the destination slots and must clear them when finished.
    /// </remarks>
    ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken);

    /// <summary>Atomically claims an eligible row, or returns false when the row is absent or ineligible.</summary>
    /// <remarks>
    /// Use a new nonempty token for every attempt; never reuse tokens. Evaluate eligibility at now using the same
    /// rules as ReadPendingAsync. Set the token and expiresAt only when no live claim exists. Reject empty IDs/tokens
    /// and expiresAt at or before now. Providers compare all timestamps as UTC instants.
    /// </remarks>
    ValueTask<bool> TryClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken);

    /// <summary>Extends a matching live claim; false means ownership is lost or the row no longer exists.</summary>
    /// <remarks>Reject empty IDs/tokens and expiresAt at or before now. Never shorten an existing expiry.</remarks>
    ValueTask<bool> RenewClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken);

    /// <summary>Deletes a successfully sent row only if its token matches and its claim is live at now.</summary>
    /// <remarks>Return false without mutation on absent rows, expired claims or token mismatch. Reject empty IDs/tokens.</remarks>
    ValueTask<bool> CompleteAsync(OutboxClaim claim, DateTimeOffset now, CancellationToken cancellationToken);

    /// <summary>Increments attempts, records the error and releases a matching live claim atomically.</summary>
    /// <remarks>
    /// Return false without mutation on absent rows, expired claims or token mismatch. Reject empty IDs/tokens
    /// and a null error. Repeating this operation with the released token must not increment attempts again.
    /// Poison-row movement and key parking are separate operations supplied by the poison-row integration.
    /// </remarks>
    ValueTask<bool> RecordFailureAsync(OutboxClaim claim, string error, DateTimeOffset now,
        CancellationToken cancellationToken);

    /// <summary>Counts eligible rows across all lanes at now, or returns null when the count is unavailable.</summary>
    /// <remarks>
    /// Use the same eligibility rules as ReadPendingAsync, without a lane or ID filter. Count rows, not keys or
    /// lanes. A zero count means a successful query with no eligible rows; never translate an error into zero.
    /// </remarks>
    ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken);
}

/// <summary>A bounded scan of one lane at a fixed instant. A null lane selects unordered rows.</summary>
public readonly record struct OutboxReadRequest(int? Lane, DateTimeOffset Now, Guid? AfterId = null);

/// <summary>A row identity and a unique fencing token for one dispatch attempt.</summary>
public readonly record struct OutboxClaim(Guid MessageId, Guid Token);
