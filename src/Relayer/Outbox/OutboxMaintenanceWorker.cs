using Microsoft.Extensions.Hosting;
using Relayer.Transports;

namespace Relayer;

// Providers keep admission, native SQL, bounded traversal and transaction/lease boundaries.
// The host lifecycle schedules recovery and the shared bounded retention turns.
internal abstract class OutboxMaintenanceWorker(int tableCount, int batchSize, int batchLimit, int enabledTables,
    TimeSpan sweepInterval, TimeSpan cleanupInterval, TimeSpan leaseDuration, TimeProvider? idleTimeProvider,
    OutboxDepth? depth = null, IHostApplicationLifetime? hostLifetime = null) : BackgroundService
{
    private int _cleanupStart;

    internal bool CleanupPending
    {
        get; private set;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return RunMaintenanceAsync(stoppingToken, stoppingToken);
    }

    // PostgreSQL local delivery can wake idle maintenance without cancelling an active delivery.
    protected virtual bool MaintenanceStopping => false;

    protected async Task RunMaintenanceAsync(CancellationToken stoppingToken, CancellationToken idleToken)
    {
        if (!await OutboxHostStartup.WaitAsync(hostLifetime, idleToken).ConfigureAwait(false))
        {
            return;
        }

        var clock = idleTimeProvider ?? TimeProvider.System;
        var start = clock.GetTimestamp();
        var nextSweep = TimeSpan.Zero;
        var nextCleanup = TimeSpan.Zero;
        var nextDepth = TimeSpan.Zero;

        try
        {
            while (!stoppingToken.IsCancellationRequested && !MaintenanceStopping)
            {
                if (clock.GetElapsedTime(start) >= nextSweep)
                {
                    var scanned = 0;

                    try
                    {
                        scanned = await RecoverAsync(stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception error)
                    {
                        ReportFailure("sweeper", error);
                    }

                    // Drain the fixed scan before delaying retries. Cleanup and gauge wakeups
                    // during the delay leave this recovery deadline in place.
                    nextSweep = clock.GetElapsedTime(start) + (scanned == 0 ? sweepInterval : TimeSpan.Zero);
                }

                if (MaintenanceStopping)
                {
                    break;
                }

                if (clock.GetElapsedTime(start) >= nextCleanup)
                {
                    try
                    {
                        await CleanupAsync(stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception error)
                    {
                        ReportFailure("cleanup", error);
                    }

                    // A full cleanup turn resumes only after the next recovery turn.
                    nextCleanup = CleanupPending ? nextSweep : clock.GetElapsedTime(start) + cleanupInterval;
                }

                // Gauges read the cached counts; the store is queried only while a listener observes them.
                if (depth is { Enabled: true } && clock.GetElapsedTime(start) >= nextDepth)
                {
                    try
                    {
                        await RefreshDepthAsync(stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception error)
                    {
                        ReportFailure("depth", error);
                    }

                    nextDepth = clock.GetElapsedTime(start) + cleanupInterval;
                }

                var next = nextSweep < nextCleanup ? nextSweep : nextCleanup;

                if (depth is { Enabled: true } && nextDepth < next)
                {
                    next = nextDepth;
                }

                var delay = next - clock.GetElapsedTime(start);

                if (delay <= TimeSpan.Zero)
                {
                    continue;
                }

                if (clock == TimeProvider.System)
                {
                    // Round up to avoid spinning before the deadline.
                    await Task.Delay(TimeSpan.FromMilliseconds(Math.Ceiling(delay.TotalMilliseconds)), idleToken).ConfigureAwait(false);
                }
                else if (delay.Ticks % TimeSpan.TicksPerMillisecond == 0)
                {
                    await Task.Delay(delay, clock, idleToken).ConfigureAwait(false);
                }
                else
                {
                    await WaitForCustomTimerAsync(delay, clock, idleToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (idleToken.IsCancellationRequested)
        {
        }
        finally
        {
            using var timeout = new CancellationTokenSource(leaseDuration);

            try
            {
                await ReleaseMaintenanceAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (Exception error)
            {
                ReportFailure("lease release", error);
            }
        }
    }

    private static async ValueTask WaitForCustomTimerAsync(TimeSpan delay, TimeProvider clock, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Task.Delay truncates fractional milliseconds before calling the provider.
        var elapsed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ITimer timer;

        using (ExecutionContext.IsFlowSuppressed() ? default : ExecutionContext.SuppressFlow())
        {
            timer = clock.CreateTimer(static state => ((TaskCompletionSource)state!).TrySetResult(),
                elapsed, delay, Timeout.InfiniteTimeSpan);
        }

        using (timer)
        {
            await elapsed.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal async ValueTask RefreshDepthAsync(CancellationToken cancellationToken = default)
    {
        if (depth is null)
        {
            return;
        }

        var (dead, parked) = await CountDepthAsync(cancellationToken).ConfigureAwait(false);
        depth.Set(dead, parked);
    }

    // Store-wide counts of dead rows and parked ordered rows. Every maintenance instance reads them.
    protected abstract ValueTask<(long Dead, long Parked)> CountDepthAsync(CancellationToken cancellationToken);

    public override void Dispose()
    {
        depth?.Dispose();
        base.Dispose();
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

                if ((active & bit) == 0)
                {
                    continue;
                }

                var deleted = await DeleteRetentionBatchAsync(table, cancellationToken).ConfigureAwait(false);

                if (deleted < 0)
                {
                    CleanupPending = false;
                    return count;
                }

                count += deleted;

                if (deleted < batchSize)
                {
                    active &= ~bit;
                }
            }
        }
        // A full final batch may have more behind it. Yield to recovery before admitting more.
        CleanupPending = active != 0;
        return count;
    }

    // A negative result means the provider did not acquire admission; stop this turn.
    protected abstract ValueTask<int> DeleteRetentionBatchAsync(int table, CancellationToken cancellationToken);
}
