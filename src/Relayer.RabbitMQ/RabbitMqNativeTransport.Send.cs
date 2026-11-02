using System.Collections.Frozen;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        Endpoint(message.Destination).Tracing is { } tracing
            ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        var endpoint = Endpoint(message.Destination);
        if (message.IsTombstone) throw new NotSupportedException("RabbitMQ does not support native tombstones.");
        if (message.OrderingKey is not null) throw new NotSupportedException("RabbitMQ publishing does not provide ordering by key.");
        var delay = endpoint.Delay(message.DeliverAt);
        var publishers = Volatile.Read(ref _publishers) ?? throw new InvalidOperationException("Start the RabbitMQ transport before sending.");
        var connection = _connection ?? throw new InvalidOperationException("Start the RabbitMQ transport before sending.");
        var publisher = await publishers.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        var started = Stopwatch.GetTimestamp();
        var failed = true;
        try
        {
            if (!publisher.Channel.IsOpen)
            {
                await publisher.Channel.DisposeAsync().ConfigureAwait(false);
                publisher.Channel = await connection.CreateChannelAsync(_publisherChannelOptions, cancellationToken).ConfigureAwait(false);
            }
            publisher.Prepare(message, delay);
            // Native tracking waits for ack and propagates nack, basic.return and channel/connection failures.
            await publisher.Channel.BasicPublishAsync(delay > 0 ? endpoint.DelayedExchange! : endpoint.Exchange,
                endpoint.RoutingKey, mandatory: delay == 0, publisher.Properties, message.Body, cancellationToken).ConfigureAwait(false);
            failed = false;
        }
        finally
        {
            endpoint.Telemetry?.RecordDuration(Stopwatch.GetElapsedTime(started), failed);
            publisher.Clear();
            publishers.Writer.TryWrite(publisher);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        // There is no native batch publish: each confirmed native call gets its own send span and duration.
        for (var i = 0; i < messages.Length; i++)
            await SendAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        // AMQP 0-9-1 has no atomic batch publish. Preserve native sequential partial-success semantics.
        for (var i = 0; i < messages.Length; i++)
            await SendCoreAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
    }
}
