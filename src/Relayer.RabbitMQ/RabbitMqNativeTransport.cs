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

    internal RabbitMqTransportOptions Options => _options;
    internal List<RabbitMqConsumer> Consumers { get; } = [];
    internal bool HealthTracking { get; set; }
    internal bool IsConnected => _connection?.IsOpen == true;
    public string Name { get; }

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        if (_endpoints.Count == 0) return RabbitMqCapabilities.GetCapabilities(endpoint);
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
            if (_publishers is not null) return;
            var publishers = System.Threading.Channels.Channel.CreateBounded<RabbitMqPublisher>(
                new System.Threading.Channels.BoundedChannelOptions(_publisherCount)
                { AllowSynchronousContinuations = false });
            try
            {
                for (var i = 0; i < _publisherCount; i++)
                    publishers.Writer.TryWrite(new(await _connection.CreateChannelAsync(
                        _publisherChannelOptions, cancellationToken).ConfigureAwait(false)));
                Volatile.Write(ref _publishers, publishers);
                foreach (var consumer in Consumers)
                {
                    consumer.TrackHealth = HealthTracking;
                    await consumer.StartAsync(_connection, cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                Volatile.Write(ref _publishers, null);
                foreach (var consumer in Consumers) await consumer.StopAsync(cancellationToken).ConfigureAwait(false);
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
            try { await CloseAsync().ConfigureAwait(false); }
            finally { foreach (var consumer in Consumers) consumer.DisposeCancellation(); }
        }
        finally { _lifecycle.Release(); }
    }

    private async ValueTask CloseAsync(CancellationToken cancellationToken = default)
    {
        System.Threading.Channels.Channel<RabbitMqPublisher>? publishers = null;
        var connection = _connection;
        try
        {
            foreach (var consumer in Consumers) await consumer.StopAsync(cancellationToken).ConfigureAwait(false);
            publishers = Interlocked.Exchange(ref _publishers, null);
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
            publishers ??= Interlocked.Exchange(ref _publishers, null);
            publishers?.Writer.TryComplete();
            _connection = null;
            if (connection is not null) await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
