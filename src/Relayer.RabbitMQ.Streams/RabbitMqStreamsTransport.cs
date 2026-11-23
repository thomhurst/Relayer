using Microsoft.Extensions.Logging;
using RabbitMQ.Stream.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ.Streams;

internal sealed class RabbitMqStreamsTransport : ITransport
{
    private readonly StreamSystemConfig _configuration;
    private readonly ILogger<StreamSystem>? _logger;
    private readonly RabbitMqStream[] _streams;
    private readonly RabbitMqSuperStream[] _superStreams;
    // Serializes lifecycle and topology only. No message path is implemented.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private StreamSystem? _system;
    private bool _disposed;
    private static readonly TransportCapabilities s_capabilities = new() { ConsumptionModel = ConsumptionModel.Log };

    internal RabbitMqStreamsTransport(string name, RabbitMqStreamsOptions options, ILogger<StreamSystem>? logger)
    {
        Name = name;
        _logger = logger;
        ArgumentNullException.ThrowIfNull(options.Configuration);
        _configuration = options.Configuration;
        _streams = options.Streams.ToArray();
        _superStreams = options.SuperStreams.ToArray();
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var stream in _streams)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentException.ThrowIfNullOrWhiteSpace(stream.Name);

            if (!names.Add(stream.Name))
            {
                throw new ArgumentException($"Duplicate RabbitMQ stream '{stream.Name}'.");
            }

            if (stream.Configuration is { } config && config.Name != stream.Name)
            {
                throw new ArgumentException($"RabbitMQ stream '{stream.Name}': native StreamSpec.Name must match the declaration.");
            }
        }

        names.Clear();

        foreach (var superStream in _superStreams)
        {
            ArgumentNullException.ThrowIfNull(superStream);
            ArgumentException.ThrowIfNullOrWhiteSpace(superStream.Name);

            if (!names.Add(superStream.Name))
            {
                throw new ArgumentException($"Duplicate RabbitMQ Streams topology declaration '{superStream.Name}'.");
            }

            if (superStream.Configuration is { } config && config.Name != superStream.Name)
            {
                throw new ArgumentException($"RabbitMQ super stream '{superStream.Name}': native SuperStreamSpec.Name must match the declaration.");
            }
        }
    }

    public string Name
    {
        get;
    }

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return s_capabilities;
    }

    public async ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        if (!Enum.IsDefined(mode))
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (mode == TopologyMode.None || (_streams.Length == 0 && _superStreams.Length == 0))
            {
                return new([]);
            }

            await ConnectAsync(cancellationToken).ConfigureAwait(false);
            var system = _system!;
            var differences = new List<string>();

            if (mode == TopologyMode.Provision)
            {
                await ProvisionStreamsAsync(system, differences, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await ValidateStreamsAsync(system, mode, differences, cancellationToken).ConfigureAwait(false);
            }

            foreach (var superStream in _superStreams)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await ApplySuperStreamTopologyAsync(system, superStream, mode, differences, cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
            }

            if (mode == TopologyMode.Provision)
            {
                // Name-only stream declarations can refer to partitions created by a super stream above.
                await ValidateStreamsAsync(system, mode, differences, cancellationToken).ConfigureAwait(false);
            }

            return new(differences);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private async Task ProvisionStreamsAsync(StreamSystem system, List<string> differences, CancellationToken cancellationToken)
    {
        foreach (var stream in _streams)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (stream.Configuration is not { } config)
            {
                continue;
            }

            try
            {
                await system.CreateStream(config).ConfigureAwait(false);
            }
            catch (CreateStreamException error)
            {
                differences.Add($"Stream '{stream.Name}': {error.Message}. Reconcile native settings externally; Relayer never updates or deletes streams.");
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private async Task ValidateStreamsAsync(StreamSystem system, TopologyMode mode, List<string> differences,
        CancellationToken cancellationToken)
    {
        foreach (var stream in _streams)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (mode == TopologyMode.Provision && stream.Configuration is not null)
            {
                continue;
            }

            if (!await system.StreamExists(stream.Name).ConfigureAwait(false))
            {
                differences.Add($"Stream '{stream.Name}' does not exist. Supply native Configuration and select TopologyMode.Provision, or create it externally.");
            }
            else if (stream.Configuration is not null)
            {
                differences.Add($"Stream '{stream.Name}' exists, but read-only configuration equivalence checks are unavailable. Use a name-only declaration for existence validation or Provision for native create-only equivalence checks.");
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private static async Task ApplySuperStreamTopologyAsync(StreamSystem system, RabbitMqSuperStream declaration,
        TopologyMode mode, List<string> differences, CancellationToken cancellationToken)
    {
        var name = declaration.Name;

        if (mode == TopologyMode.Provision && declaration.Configuration is { } config)
        {
            try
            {
                await system.CreateSuperStream(config).ConfigureAwait(false);
            }
            catch (CreateStreamException error)
            {
                differences.Add($"Super stream '{name}': {error.Message}. Reconcile native settings externally; Relayer never updates or deletes super streams.");
                return;
            }
            catch (ArgumentException error)
            {
                differences.Add($"Super stream '{name}': {error.Message}. Correct the native configuration and retry.");
                return;
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        // The native existence check also accepts an exchange with zero stream partitions.
        var exists = await system.SuperStreamExists(name).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        if (!exists || (await system.QueryPartition(name).ConfigureAwait(false)).Length == 0)
        {
            differences.Add($"Super stream '{name}' has no native partitions. Supply native Configuration and select TopologyMode.Provision, or provision the partitions and bindings externally.");
        }
        else if (mode == TopologyMode.Validate && declaration.Configuration is not null)
        {
            differences.Add($"Super stream '{name}' exists, but read-only configuration equivalence checks are unavailable. Use a name-only declaration and verify native settings externally.");
        }
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
        catch
        {
            await CloseAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private async Task ConnectAsync(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        _system ??= await StreamSystem.Create(_configuration, _logger).ConfigureAwait(false);
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
        // Close an idle system even if an earlier hosted service used the shutdown budget.
        if (!_lifecycle.Wait(0))
        {
            await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            await CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);

        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            await CloseAsync().ConfigureAwait(false);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private async Task CloseAsync()
    {
        var system = _system;
        _system = null;

        if (system is not null)
        {
            await system.DisposeAsync().ConfigureAwait(false);
        }
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("RabbitMQ Streams send execution is not implemented (#316).");
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("RabbitMQ Streams send execution is not implemented (#316).");
    }
}
