using Microsoft.Extensions.Hosting;

namespace Relayer.Transports;

// Owns a transport's consumer lifetime for the host and for its keyed IConsumerControl. One gate serialises both callers.
// A transport whose consumers can fail outside the native client reports that failure through ExecuteAsync.
internal abstract class ConsumerHostedService(string transport) : BackgroundService, IConsumerControl
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    // Cancels a control start still running when host shutdown bypasses the gate, so it cannot start consumers
    // after the shutdown stop pass.
    private CancellationTokenSource _shutdown = new();
    private bool _hostRunning;
    private bool _consuming;

    /// <summary>Composes and validates the consumers once; a retried host start must not compose them twice.</summary>
    protected abstract Task ComposeAsync(CancellationToken cancellationToken);

    protected abstract bool AutoStartConsumers
    {
        get;
    }

    protected abstract Task StartConsumersAsync(CancellationToken cancellationToken);

    /// <summary>Stops and drains the consumers. Called on host shutdown even when nothing was composed.</summary>
    protected abstract Task StopConsumersAsync(CancellationToken cancellationToken);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.CompletedTask;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_shutdown.IsCancellationRequested)
            {
                _shutdown.Dispose();
                _shutdown = new();
            }

            await ComposeAsync(cancellationToken).ConfigureAwait(false);

            if (AutoStartConsumers)
            {
                await StartAsyncCore(cancellationToken).ConfigureAwait(false);
            }

            _hostRunning = true;
        }
        finally
        {
            _gate.Release();
        }

        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Shutdown must reach the consumers even when its deadline has passed or a control call holds the gate:
        // the consumer's stop aborts in-flight work on an expired token and still stops the native receive.
        Volatile.Write(ref _hostRunning, false);
        _shutdown.Cancel();
        var entered = false;

        try
        {
            try
            {
                await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                entered = true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }

            if (entered)
            {
                await StopAsyncCore(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _consuming = false;
                await StopConsumersAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (entered)
            {
                _gate.Release();
            }

            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    async Task IConsumerControl.StartAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!Volatile.Read(ref _hostRunning))
            {
                throw new InvalidOperationException($"{transport}: start the host before starting its consumers.");
            }

            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
            await StartAsyncCore(linked.Token).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    async Task IConsumerControl.StopAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await StopAsyncCore(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task StartAsyncCore(CancellationToken cancellationToken)
    {
        if (_consuming)
        {
            return;
        }

        await StartConsumersAsync(cancellationToken).ConfigureAwait(false);
        _consuming = true;
    }

    private async Task StopAsyncCore(CancellationToken cancellationToken)
    {
        if (!_consuming)
        {
            return;
        }
        // A failed or expired drain still counts as stopped; the consumer itself rejects a restart after an abort.
        _consuming = false;
        await StopConsumersAsync(cancellationToken).ConfigureAwait(false);
    }
}
