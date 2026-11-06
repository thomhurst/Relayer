using RabbitMQ.Stream.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ.Streams;

internal sealed class RabbitMqStreamsTransport : ITransport
{
    private readonly StreamSystemConfig _configuration;
    private readonly RabbitMqStream[] _streams;
    // Serializes lifecycle and topology only. No message path is implemented.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private StreamSystem? _system;
    private bool _disposed;
    private static readonly TransportCapabilities Capabilities = new() { ConsumptionModel = ConsumptionModel.Log };

    internal RabbitMqStreamsTransport(string name, RabbitMqStreamsOptions options)
    {
        Name = name;
        ArgumentNullException.ThrowIfNull(options.Configuration);
        _configuration = options.Configuration;
        _streams = options.Streams.ToArray();
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var stream in _streams)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(stream.Name);
            if (!names.Add(stream.Name)) throw new ArgumentException($"Duplicate RabbitMQ stream '{stream.Name}'.");
            if (stream.Configuration is { } config && config.Name != stream.Name)
                throw new ArgumentException($"RabbitMQ stream '{stream.Name}': native StreamSpec.Name must match the declaration.");
        }
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
            if (mode == TopologyMode.None || _streams.Length == 0) return new([]);
            await ConnectAsync(cancellationToken).ConfigureAwait(false);
            var differences = new List<string>();
            foreach (var stream in _streams)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (mode == TopologyMode.Provision && stream.Configuration is { } config)
                {
                    try { await _system!.CreateStream(config).ConfigureAwait(false); }
                    catch (CreateStreamException error)
                    {
                        differences.Add($"Stream '{stream.Name}': {error.Message}. Reconcile native settings externally; Relayer never updates or deletes streams.");
                    }
                }
                else if (!await _system!.StreamExists(stream.Name).ConfigureAwait(false))
                    differences.Add($"Stream '{stream.Name}' does not exist. Supply native Configuration and select TopologyMode.Provision, or create it externally.");
                else if (stream.Configuration is not null)
                    differences.Add($"Stream '{stream.Name}' exists, but read-only configuration equivalence checks are unavailable. Use a name-only declaration for existence validation or Provision for native create-only equivalence checks.");
                cancellationToken.ThrowIfCancellationRequested();
            }
            return new(differences);
        }
        finally { _lifecycle.Release(); }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            await ConnectAsync(cancellationToken).ConfigureAwait(false);
            // Native ServerProperties recovers an observed disconnect before returning cached metadata.
            await _system!.ServerProperties().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch { await CloseAsync().ConfigureAwait(false); throw; }
        finally { _lifecycle.Release(); }
    }

    private async Task ConnectAsync(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        _system ??= await StreamSystem.Create(_configuration).ConfigureAwait(false);
        // Native operations have no CancellationToken. Drain them before releasing ownership;
        // never abandon a pending connect that could later leak an owned connection.
        if (token.IsCancellationRequested)
        {
            await CloseAsync().ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
        }
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

    private async Task CloseAsync()
    {
        var system = _system;
        _system = null;
        if (system is not null) await system.DisposeAsync().ConfigureAwait(false);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        throw new NotSupportedException("RabbitMQ Streams send execution is not implemented (#316).");

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) =>
        throw new NotSupportedException("RabbitMQ Streams send execution is not implemented (#316).");
}
