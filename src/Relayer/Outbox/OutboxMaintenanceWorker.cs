using Microsoft.Extensions.Hosting;

namespace Relayer;

// Only retention scheduling is shared. Each provider keeps admission, native SQL and
// transaction/lease boundaries inside DeleteRetentionBatchAsync.
internal abstract class OutboxMaintenanceWorker(int tableCount, int batchSize, int batchLimit, int enabledTables) : BackgroundService
{
    private int _cleanupStart;
    internal bool CleanupPending { get; private set; }

    internal async ValueTask<int> CleanupAsync(CancellationToken cancellationToken = default)
    {
        var count = 0;
        var active = enabledTables;
        // Rotate before awaiting: a busy table or cancellation cannot monopolize subsequent turns.
        var first = _cleanupStart;
        _cleanupStart = (first + 1) % tableCount;
        // Only a successful turn may request an immediate continuation.
        CleanupPending = false;
        for (var round = 0; round < batchLimit && active != 0; round++)
        {
            for (var offset = 0; offset < tableCount; offset++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var table = (first + offset) % tableCount;
                var bit = 1 << table;
                if ((active & bit) == 0) continue;
                var deleted = await DeleteRetentionBatchAsync(table, cancellationToken).ConfigureAwait(false);
                if (deleted < 0) { CleanupPending = false; return count; }
                count += deleted;
                if (deleted < batchSize) active &= ~bit;
            }
        }
        // A full final batch may have more behind it. Yield to recovery before admitting more.
        CleanupPending = active != 0;
        return count;
    }

    // A negative result means the provider did not acquire admission; stop this turn.
    protected abstract ValueTask<int> DeleteRetentionBatchAsync(int table, CancellationToken cancellationToken);
}
