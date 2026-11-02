using RabbitMQ.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

internal sealed class RabbitMqNativeTransport : ITransport
{
    private readonly ConnectionFactory _factory;
    private readonly RabbitMqTopology _topology;
    // Startup, topology and shutdown only. No per-message synchronization is introduced here.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private IConnection? _connection;
    private bool _disposed;

    internal RabbitMqNativeTransport(string name, RabbitMqTransportOptions options)
    {
        Name = name;
        ArgumentNullException.ThrowIfNull(options.ConnectionFactory);
        _factory = options.ConnectionFactory;
        _topology = new(options);
    }

    public string Name { get; }

    public TransportCapabilities GetCapabilities(string endpoint) => RabbitMqTransport.GetCapabilities(endpoint);

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
        }
        finally { _lifecycle.Release(); }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { await CloseAsync().ConfigureAwait(false); }
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

    private async ValueTask CloseAsync()
    {
        var connection = _connection;
        _connection = null;
        if (connection is not null) await connection.DisposeAsync().ConfigureAwait(false);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        throw new NotSupportedException("RabbitMQ send execution is not implemented yet.");

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) =>
        throw new NotSupportedException("RabbitMQ send execution is not implemented yet.");
}
