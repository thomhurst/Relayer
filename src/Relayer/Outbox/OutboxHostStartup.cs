using Microsoft.Extensions.Hosting;

namespace Relayer;

internal static class OutboxHostStartup
{
    internal static async ValueTask<bool> WaitAsync(IHostApplicationLifetime? hostLifetime, CancellationToken stoppingToken)
    {
        // Hosted transports can still be opening their publishers when a background worker starts.
        if (hostLifetime?.ApplicationStarted is { IsCancellationRequested: false } started)
        {
            var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = started.UnsafeRegister(static state => ((TaskCompletionSource)state!).SetResult(), ready);

            try
            {
                await ready.Task.WaitAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return false;
            }
        }

        return !stoppingToken.IsCancellationRequested;
    }
}
