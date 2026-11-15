namespace Relayer;

// Cuts an ordered worker's idle poll short after an in-process commit of a keyed row.
// Commits from other processes still wait for the poll interval.
internal sealed class OrderedLaneWake
{
    private TaskCompletionSource? _waiter;
    private int _signalled;

    // Called on the committing thread. The waiter continues asynchronously, never inline here.
    internal void Signal()
    {
        if (Interlocked.Exchange(ref _signalled, 1) == 0) Volatile.Read(ref _waiter)?.TrySetResult();
    }

    // A signal raised during the previous scan returns at once, so its rows are probed next.
    internal async Task WaitAsync(TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _signalled, 0) != 0) return;
        var waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Interlocked.Exchange(ref _waiter, waiter);
        try
        {
            if (Volatile.Read(ref _signalled) != 0) return;
            using var delay = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var timer = Task.Delay(timeout, timeProvider, delay.Token);
            if (await Task.WhenAny(waiter.Task, timer).ConfigureAwait(false) == timer)
                await timer.ConfigureAwait(false);
            else
                await delay.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _waiter, null);
            // Every signal so far precedes the scan that follows this wait.
            Volatile.Write(ref _signalled, 0);
        }
    }
}
