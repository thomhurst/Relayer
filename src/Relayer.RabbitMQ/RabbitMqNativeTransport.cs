using RabbitMQ.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

internal sealed partial class RabbitMqNativeTransport : ITransport
{
    private readonly ConnectionFactory _factory;
    private readonly RabbitMqTopology _topology;
    // Startup, topology and shutdown only. No per-message synchronization is introduced here.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private IConnection? _connection;
    private bool _disposed;

    public string Name { get; }

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        if (_endpoints.Count == 0) return RabbitMqTransport.GetCapabilities(endpoint);
        var destination = Endpoint(endpoint);
        if (!_initialized && _options.DelayedExchanges.ContainsKey(destination.Exchange))
            throw new InvalidOperationException("RabbitMQ delayed exchange capabilities are not initialized. Start the application host before resolving send routes.");
        return destination.Capabilities;
    }

    public async ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (mode == TopologyMode.None) return new([]);
            _connection ??= await _factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            return await _topology.ApplyAsync(_connection, mode, cancellationToken).ConfigureAwait(false);
        }
        finally { _lifecycle.Release(); }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _connection ??= await _factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            if (_publishers is not null || _endpoints.Count == 0) return;
            var publishers = System.Threading.Channels.Channel.CreateBounded<RabbitMqPublisher>(
                new System.Threading.Channels.BoundedChannelOptions(_publisherCount)
                { AllowSynchronousContinuations = false });
            try
            {
                for (var i = 0; i < _publisherCount; i++)
                    publishers.Writer.TryWrite(new(await _connection.CreateChannelAsync(
                        _publisherChannelOptions, cancellationToken).ConfigureAwait(false)));
                Volatile.Write(ref _publishers, publishers);
            }
            catch
            {
                while (publishers.Reader.TryRead(out var publisher))
                    await publisher.Channel.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally { _lifecycle.Release(); }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { await CloseAsync(cancellationToken).ConfigureAwait(false); }
        finally { _lifecycle.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed) return;
            _disposed = true;
            await CloseAsync().ConfigureAwait(false);
        }
        finally { _lifecycle.Release(); }
    }

    private async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
        var publishers = Interlocked.Exchange(ref _publishers, null);
        var connection = _connection;
        try
        {
            if (publishers is not null)
            {
                // Stop admission first. Drain borrowed channels before disposing the connection.
                for (var i = 0; i < _publisherCount; i++)
                {
                    var publisher = await publishers.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    await publisher.Channel.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        finally
        {
            publishers?.Writer.TryComplete();
            _connection = null;
            if (connection is not null) await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
