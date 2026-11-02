using NATS.Client.Core;
using NATS.Client.JetStream;
using Relayer.Transports;

namespace Relayer.Nats;

internal sealed class NatsNativeTransport : ITransport
{
    private readonly NatsOpts _options;
    private readonly NatsJSOpts? _jetStreamOptions;
    private readonly NatsTopology _topology;
    // Only startup, topology and shutdown use this gate. No message path is implemented.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private NatsConnection? _connection;
    private NatsJSContext? _context;
    private bool _disposed;
    private static readonly TransportCapabilities Capabilities = new();

    internal NatsNativeTransport(string name, NatsTransportOptions options)
    {
        Name = name;
        ArgumentNullException.ThrowIfNull(options.ConnectionOptions);
        _options = options.ConnectionOptions;
        _jetStreamOptions = options.JetStreamOptions;
        _topology = new(options);
    }

    public string Name { get; }
    public TransportCapabilities GetCapabilities(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return Capabilities;
    }

    public async ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (mode == TopologyMode.None) return new([]);
            EnsureConnection();
            try { return await _topology.ApplyAsync(_context!, mode, cancellationToken).ConfigureAwait(false); }
            catch
            {
                // Native initial connect has no cancellation token. Closing our connection also cancels that work.
                await CloseAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally { _lifecycle.Release(); }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            EnsureConnection();
            try { await _connection!.PingAsync(cancellationToken).ConfigureAwait(false); }
            catch { await CloseAsync().ConfigureAwait(false); throw; }
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

    private void EnsureConnection()
    {
        if (_connection is not null) return;
        _connection = new(_options);
        _context = _jetStreamOptions is null ? new(_connection) : new(_connection, _jetStreamOptions);
    }

    private async ValueTask CloseAsync()
    {
        var connection = _connection;
        _connection = null;
        _context = null;
        if (connection is not null) await connection.DisposeAsync().ConfigureAwait(false);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        throw new NotSupportedException("NATS send execution is not implemented yet (#313).");

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) =>
        throw new NotSupportedException("NATS send execution is not implemented yet (#313).");
}
