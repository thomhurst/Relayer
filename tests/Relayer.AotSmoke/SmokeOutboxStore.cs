using Relayer;

// Exercise keyed scoped store resolution and the optional adapter with reflection disabled.
public sealed class SmokeOutboxStore : IOutboxStore
{
    public ValueTask<long?> GetPendingCountAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (now == default || now.Offset != TimeSpan.Zero)
            throw new InvalidOperationException("Outbox health did not receive a current UTC instant.");
        return ValueTask.FromResult<long?>(5);
    }

    public ValueTask<int> ReadPendingAsync(OutboxReadRequest request, Memory<OutboxRecord> destination,
        CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask<bool> TryClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask<bool> RenewClaimAsync(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expiresAt,
        CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask<bool> CompleteAsync(OutboxClaim claim, DateTimeOffset now,
        CancellationToken cancellationToken) => throw new NotSupportedException();
    public ValueTask<bool> RecordFailureAsync(OutboxClaim claim, string error, DateTimeOffset now,
        CancellationToken cancellationToken) => throw new NotSupportedException();
}
