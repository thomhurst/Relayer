using System.Runtime.CompilerServices;

namespace Relayer;

// Provider callbacks run only between batches. Native row admission, fencing, transactions and
// completion remain in each provider's DispatchLaneAsync implementation.
internal interface IOrderedLaneWorker
{
    int Lanes { get; }
    bool StopRequested { get; }
    bool IsReady(int lane);
    bool IsAssigned(int lane);
    ValueTask FindReadyAsync(CancellationToken cancellationToken);
    ValueTask RebalanceAsync(CancellationToken cancellationToken);
    ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken);
    ValueTask ReleaseAsync(CancellationToken cancellationToken);
    void DispatchFailed(Exception error);
    void ReleaseFailed(Exception error);
}

// Shared ownership cadence, sequential lane traversal, batch deadlines and drain-before-release.
internal static class OrderedLaneWorker
{
    internal static async Task RunAsync(IOrderedLaneWorker worker, TimeSpan leaseDuration, TimeSpan pollInterval, TimeProvider timeProvider, CancellationToken stoppingToken, CancellationToken idleToken)
    {
        // Reuse the deadline source across successful batches. Cancellation is an exceptional path.
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        long? lastRebalance = null;
        try
        {
            while (!stoppingToken.IsCancellationRequested && !worker.StopRequested)
            {
                var madeProgress = false;
                try
                {
                    await RebalanceIfDueAsync().ConfigureAwait(false);
                    await worker.FindReadyAsync(stoppingToken).ConfigureAwait(false);
                    for (var lane = 0; lane < worker.Lanes; lane++)
                    {
                        if (worker.StopRequested) break;
                        if (!worker.IsReady(lane) || !worker.IsAssigned(lane)) continue;
                        // A preceding slow batch can consume the heartbeat interval. Refresh before renewing another lane.
                        await RebalanceIfDueAsync().ConfigureAwait(false);
                        if (!worker.IsAssigned(lane)) continue;
                        deadline.CancelAfter(leaseDuration / 2);
                        try
                        {
                            madeProgress |= await worker.DispatchLaneAsync(lane, deadline.Token).ConfigureAwait(false) > 0;
                        }
                        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { throw; }
                        catch (Exception error) { worker.DispatchFailed(error); }
                        finally
                        {
                            deadline.CancelAfter(Timeout.InfiniteTimeSpan);
                            if (!deadline.TryReset())
                            {
                                deadline.Dispose();
                                deadline = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error) { worker.DispatchFailed(error); }
                if (!madeProgress)
                    await Task.Delay(pollInterval, timeProvider, idleToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (idleToken.IsCancellationRequested) { }
        finally
        {
            deadline.Dispose();
            using var cleanup = new CancellationTokenSource(leaseDuration / 2);
            try { await worker.ReleaseAsync(cleanup.Token).ConfigureAwait(false); }
            catch (Exception error) { worker.ReleaseFailed(error); }
        }

        [RuntimeAsyncMethodGeneration(false)]
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        async ValueTask RebalanceIfDueAsync()
        {
            if (lastRebalance is { } last && timeProvider.GetElapsedTime(last) < leaseDuration / 3) return;
            var started = timeProvider.GetTimestamp();
            await worker.RebalanceAsync(stoppingToken).ConfigureAwait(false);
            lastRebalance = started;
        }
    }

}
