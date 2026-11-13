using Relayer.Transports;
namespace Relayer.HealthChecks;

internal sealed class OutboxStoreHealthProvider(IOutboxStore store, TimeProvider timeProvider) : IOutboxHealthProvider
{
    public ValueTask<long?> GetPendingCountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return store.GetPendingCountAsync(timeProvider.GetUtcNow(), cancellationToken);
    }

    public ValueTask<long?> GetPendingCountAsync(long countLimit, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(countLimit);
        return store.GetPendingCountAsync(timeProvider.GetUtcNow(), countLimit, cancellationToken);
    }
}
