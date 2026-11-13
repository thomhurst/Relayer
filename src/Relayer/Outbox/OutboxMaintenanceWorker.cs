using Microsoft.Extensions.Hosting;

namespace Relayer;

// Providers keep admission, native SQL, bounded traversal and transaction/lease boundaries.
// The host lifecycle schedules recovery and the shared bounded retention turns.
internal abstract class OutboxMaintenanceWorker(int tableCount, int batchSize, int batchLimit, int enabledTables,
    TimeSpan sweepInterval, TimeSpan cleanupInterval, TimeSpan leaseDuration, TimeProvider? idleTimeProvider) : BackgroundService
{
    private int _cleanupStart;
    internal bool CleanupPending { get; private set; }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => RunMaintenanceAsync(stoppingToken, stoppingToken);

    // PostgreSQL local delivery can wake idle maintenance without cancelling an active delivery.
    protected virtual bool MaintenanceStopping => false;

    protected async Task RunMaintenanceAsync(CancellationToken stoppingToken, CancellationToken idleToken)
    {
        var nextCleanup = DateTimeOffset.MinValue;
        try
        {
            while (!stoppingToken.IsCancellationRequested && !MaintenanceStopping)
            {
                var scanned = 0;
                try { scanned = await RecoverAsync(stoppingToken).ConfigureAwait(false); }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error) { ReportFailure("sweeper", error); }
                if (MaintenanceStopping) break;
                // Recovery failure must not suppress the independently admitted retention turn.
                if (DateTimeOffset.UtcNow >= nextCleanup)
                {
                    try { await CleanupAsync(stoppingToken).ConfigureAwait(false); }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                    catch (Exception error) { ReportFailure("cleanup", error); }
                    nextCleanup = DateTimeOffset.UtcNow + (CleanupPending ? TimeSpan.Zero : cleanupInterval);
                }
                // Native recovery returns the scanned count within its fixed boundary. Its empty
                // terminal pass provides the retry delay even when every send failed.
                if (scanned == 0)
                    await Task.Delay(sweepInterval, idleTimeProvider ?? TimeProvider.System, idleToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (idleToken.IsCancellationRequested) { }
        finally
        {
            using var timeout = new CancellationTokenSource(leaseDuration);
            try { await ReleaseMaintenanceAsync(timeout.Token).ConfigureAwait(false); }
            catch (Exception error) { ReportFailure("lease release", error); }
        }
    }

    protected abstract ValueTask<int> RecoverAsync(CancellationToken cancellationToken);
    protected abstract ValueTask ReleaseMaintenanceAsync(CancellationToken cancellationToken);
    protected abstract void ReportFailure(string job, Exception error);

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
