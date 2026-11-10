using System.Collections.Frozen;
using System.Threading.Channels;
using RabbitMQ.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ;

internal sealed partial class RabbitMqNativeTransport : ITransportSendTracing, ITransportCapabilityInitializer
{
    private readonly FrozenDictionary<string, RabbitMqSendEndpoint> _endpoints;
    private readonly RabbitMqTransportOptions _options;
    private readonly int _publisherCount;
    private readonly CreateChannelOptions _publisherChannelOptions;
    private Channel<RabbitMqPublisher>? _publishers;
    private bool _initialized;
    private readonly RabbitMqSendPool _operations = new();

    internal RabbitMqNativeTransport(string name, RabbitMqTransportOptions options,
        IEnumerable<string> destinations, MessageTracing? tracing = null, RelayerInstrumentation? instrumentation = null)
    {
        Name = name;
        ArgumentNullException.ThrowIfNull(options.ConnectionFactory);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.PublisherCount, 1);
        ArgumentNullException.ThrowIfNull(options.PublisherChannelOptions);
        if (!options.PublisherChannelOptions.PublisherConfirmationsEnabled ||
            !options.PublisherChannelOptions.PublisherConfirmationTrackingEnabled)
            throw new ArgumentException("RabbitMQ publisher channels require native publisher confirmations and confirmation tracking.");
        _factory = options.ConnectionFactory;
        _topology = new(options);
        _options = options;
        _publisherCount = options.PublisherCount;
        _publisherChannelOptions = options.PublisherChannelOptions;
        foreach (var declaration in options.DelayedExchanges.Values)
        {
            ArgumentNullException.ThrowIfNull(declaration);
            ArgumentException.ThrowIfNullOrWhiteSpace(declaration.Name);
            if (declaration.Type != "x-delayed-message")
                throw new ArgumentException("RabbitMQ DelayedExchanges declarations must use Type = 'x-delayed-message'.");
        }
        var endpoints = new Dictionary<string, RabbitMqSendEndpoint>(StringComparer.Ordinal);
        foreach (var destination in destinations)
        {
            if (!endpoints.ContainsKey(destination))
                endpoints.Add(destination, new(destination, _factory, tracing, instrumentation, SendCoreAsync, SendBatchCoreAsync));
        }
        _endpoints = endpoints.ToFrozenDictionary(StringComparer.Ordinal);
    }

    public SendTracing? GetSendTracing(string destination) => Endpoint(destination).Tracing;

    private RabbitMqSendEndpoint Endpoint(string destination) => _endpoints.TryGetValue(destination, out var endpoint)
        ? endpoint : throw new InvalidOperationException($"RabbitMQ transport '{Name}': undeclared destination '{destination}'. Declare a send route first.");

    public async ValueTask InitializeCapabilitiesAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_initialized) return;
            _connection ??= await _factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
            foreach (var endpoint in _endpoints.Values)
                await endpoint.InitializeAsync(_connection, _options, mode, cancellationToken).ConfigureAwait(false);
            _initialized = true;
        }
        finally { _lifecycle.Release(); }
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var endpoint = Endpoint(message.Destination);
        if (endpoint.TraceEnabled) return endpoint.Tracing!.SendAsync(message, cancellationToken);
        return SendCoreAsync(message, cancellationToken);
    }

    internal ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        var endpoint = Endpoint(message.Destination);
        if (message.IsTombstone) throw new NotSupportedException("RabbitMQ does not support native tombstones.");
        if (message.OrderingKey is not null) throw new NotSupportedException("RabbitMQ publishing does not provide ordering by key.");
        var delay = endpoint.Delay(message.DeliverAt);
        var publishers = Volatile.Read(ref _publishers) ?? throw new InvalidOperationException("Start the RabbitMQ transport before sending.");
        var connection = _connection ?? throw new InvalidOperationException("Start the RabbitMQ transport before sending.");
        return _operations.Rent().Start(this, endpoint, message, delay, connection, publishers, cancellationToken);
    }

    internal async ValueTask ReplacePublisherChannelAsync(RabbitMqPublisher publisher, IConnection connection, CancellationToken token)
    {
        await publisher.Channel.DisposeAsync().ConfigureAwait(false);
        publisher.Channel = await connection.CreateChannelAsync(_publisherChannelOptions, token).ConfigureAwait(false);
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        // There is no native batch publish: each confirmed native call gets its own span and duration.
        return messages.IsEmpty ? ValueTask.CompletedTask : _operations.Rent().StartBatch(this, messages, cancellationToken, traceMessages: true);
    }

    private ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        return messages.IsEmpty ? ValueTask.CompletedTask : _operations.Rent().StartBatch(this, messages, cancellationToken, traceMessages: false);
    }
}
