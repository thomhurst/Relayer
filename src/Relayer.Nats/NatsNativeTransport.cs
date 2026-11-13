using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Relayer.Transports;

namespace Relayer.Nats;

internal sealed class NatsNativeTransport : ITransport, ITransportSendTracing
{
    private readonly NatsOpts _options;
    private readonly NatsJSOpts? _jetStreamOptions;
    private readonly NatsJSPubOpts? _publishOptions;
    private readonly NatsTopology _topology;
    // Only startup, topology and shutdown use this gate.
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private NatsConnection? _connection;
    private NatsJSContext? _context;
    private bool _disposed;
    private bool _started;
    private bool _stopping;
    private static readonly TransportCapabilities Capabilities = new();
    private readonly FrozenDictionary<string, Endpoint> _endpoints;
    private readonly FrozenSet<(string Stream, string Name)> _declaredConsumers;
    private readonly MessageTracing _tracing;
    private readonly ILogger<NatsConsumerWorker>? _consumerLogger;
    internal List<NatsConsumerWorker> Consumers { get; } = [];

    internal NatsNativeTransport(string name, NatsTransportOptions options, TransportDefinition definition, MessageTracing tracing, ILoggerFactory? logging = null)
    {
        Name = name;
        _tracing = tracing;
        _consumerLogger = logging?.CreateLogger<NatsConsumerWorker>();
        ArgumentNullException.ThrowIfNull(options.ConnectionOptions);
        _options = options.ConnectionOptions;
        _jetStreamOptions = options.JetStreamOptions;
        _publishOptions = options.PublishOptions;
        _topology = new(options);
        _declaredConsumers = options.Consumers.Select(consumer => (consumer.Stream, consumer.Name)).ToFrozenSet();
        var destinations = new HashSet<string>(StringComparer.Ordinal);
        foreach (var route in definition.Routes) destinations.Add(route.Destination);
        foreach (var declaration in definition.Consumers)
        {
            var consumer = (ConsumerDefinition<NatsConsumerOptions>)declaration;
            if (!_declaredConsumers.Contains((consumer.Options.Stream, consumer.Source)))
                throw new InvalidOperationException($"NATS consumer '{consumer.Source}': declare '{consumer.Options.Stream}/{consumer.Source}' in NatsTransportOptions.Consumers and set consumer.Options.Stream.");
            ArgumentNullException.ThrowIfNull(consumer.Options.Settings);
            if (!consumer.Options.Settings.DrainOnCancel || consumer.BatchSize != 1)
                throw new NotSupportedException($"NATS consumer '{consumer.Source}': use single-message handlers and native Settings.DrainOnCancel = true.");
            ArgumentNullException.ThrowIfNull(consumer.Failure);
            if (consumer.Failure.RedeliveryDelays.Length != 0)
                throw new NotSupportedException($"NATS consumer '{consumer.Source}': NAK cannot persist failure headers. Use RetryDelays and a DeadLetterEndpoint subject.");
            if (string.IsNullOrWhiteSpace(consumer.Failure.DeadLetterEndpoint))
                throw new InvalidOperationException($"NATS consumer '{consumer.Source}': configure Failure.DeadLetterEndpoint to a subject captured by a JetStream stream. The copy is confirmed before ACK.");
            destinations.Add(consumer.Failure.DeadLetterEndpoint);
        }
        _endpoints = destinations.ToFrozenDictionary(destination => destination,
            destination => new Endpoint(new(tracing, new() { System = "nats", Destination = destination }, SendCoreAsync, SendBatchCoreAsync, headerNames: new("ce-")), _options.CaseSensitiveHeaders), StringComparer.Ordinal);
        foreach (var destination in destinations) ValidateSubject(destination);
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
                if (!_started) await CloseAsync().ConfigureAwait(false);
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
            if (_started && !_stopping) return;
            if (_stopping)
            {
                // A bounded stop may return before native drain completes. Finish that
                // lifecycle before replacing the connection and starting fresh pull loops.
                await StopConsumersAsync(cancellationToken, restarting: true).ConfigureAwait(false);
                await CloseAsync().ConfigureAwait(false);
            }
            EnsureConnection();
            try
            {
                await _connection!.PingAsync(cancellationToken).ConfigureAwait(false);
                foreach (var consumer in Consumers) await consumer.StartAsync(_context!, cancellationToken).ConfigureAwait(false);
                _started = true;
            }
            catch
            {
                try { await StopConsumersAsync(CancellationToken.None).ConfigureAwait(false); }
                finally { await CloseAsync().ConfigureAwait(false); }
                throw;
            }
        }
        finally { _lifecycle.Release(); }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _stopping = true;
            try { await StopConsumersAsync(cancellationToken).ConfigureAwait(false); }
            finally
            {
                if (Consumers.All(consumer => consumer.Completion.IsCompleted)) await CloseAsync().ConfigureAwait(false);
            }
        }
        finally { _lifecycle.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed) return;
            _disposed = true;
            try { await StopConsumersAsync(CancellationToken.None).ConfigureAwait(false); }
            finally { await CloseAsync().ConfigureAwait(false); }
        }
        finally { _lifecycle.Release(); }
    }

    private async Task StopConsumersAsync(CancellationToken token, bool restarting = false)
    {
        Exception? failure = null;
        foreach (var consumer in Consumers)
        {
            try { await consumer.StopAsync(token).ConfigureAwait(false); }
            catch (OperationCanceledException) when (restarting && consumer.Completion.IsCanceled && !token.IsCancellationRequested)
            {
                // The previous stop canceled processing; its completed cancellation is
                // expected. Native faults and cancellation of this start still propagate.
            }
            catch (Exception error) { failure ??= error; }
        }
        if (failure is not null) ExceptionDispatchInfo.Capture(failure).Throw();
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
        _started = false;
        _stopping = false;
        if (connection is not null) await connection.DisposeAsync().ConfigureAwait(false);
    }

    internal NatsConsumerWorker AddConsumer(string stream, string name, NatsJSConsumeOpts settings, int concurrency)
    {
        if (!_declaredConsumers.Contains((stream, name))) throw new InvalidOperationException($"Undeclared NATS consumer '{stream}/{name}'.");
        var worker = new NatsConsumerWorker(stream, name, settings, concurrency, _tracing, _consumerLogger);
        Consumers.Add(worker);
        return worker;
    }

    public SendTracing? GetSendTracing(string destination) => EndpointFor(destination).Tracing;
    private Endpoint EndpointFor(string destination) => _endpoints.TryGetValue(destination, out var endpoint) ? endpoint
        : throw new InvalidOperationException($"NATS transport '{Name}': declare a Send route for subject '{destination}' (or configure it as a consumer's DeadLetterEndpoint).");

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => EndpointFor(message.Destination).Tracing.SendAsync(message, cancellationToken);

    private ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        token.ThrowIfCancellationRequested();
        if (message.OrderingKey is not null || message.DeliverAt is not null || message.IsTombstone)
            throw new NotSupportedException("NATS JetStream does not implement portable keyed ordering, scheduled sends or tombstones. Use native subjects and the durable outbox for scheduling.");
        var context = _context ?? throw new InvalidOperationException("Start the NATS transport before sending.");
        var endpoint = EndpointFor(message.Destination);
        var operation = endpoint.Rent();
        var headers = operation.Headers;
        try
        {
            foreach (var header in message.Headers.Span) headers[header.Key] = header.Value.ToString();
            message.ApplyNative(headers);
            token.ThrowIfCancellationRequested();
            endpoint.Tracing.RecordNativeSend();
            headers.TryGetValue("traceparent", out headers.TraceParent);
            headers.TryGetValue("tracestate", out headers.TraceState);
            return operation.Publish(context, message, _publishOptions, token, endpoint.Tracing.StartNativeSend());
        }
        catch
        {
            headers.Reset();
            endpoint.Return(operation);
            throw;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        token.ThrowIfCancellationRequested();
        // JetStream has individual publish acknowledgements, without an atomic native batch.
        for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], token).ConfigureAwait(false);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
    {
        for (var i = 0; i < messages.Length; i++) await SendCoreAsync(messages.Span[i], token).ConfigureAwait(false);
    }

    private static void ValidateSubject(string subject)
    {
        if (string.IsNullOrWhiteSpace(subject) || subject.AsSpan().ContainsAny("*> \t\r\n") ||
            subject[0] == '.' || subject[^1] == '.' || subject.Contains("..", StringComparison.Ordinal))
            throw new ArgumentException($"NATS publish subject '{subject}' must contain nonempty tokens without whitespace or wildcards.");
    }

    internal sealed class Endpoint
    {
        private readonly NatsSendOperation?[] _operations = new NatsSendOperation?[64];
        private readonly bool _caseSensitive;
        internal Endpoint(SendTracing tracing, bool caseSensitive)
        {
            Tracing = tracing;
            _caseSensitive = caseSensitive;
            for (var i = 0; i < _operations.Length; i++) _operations[i] = new(this, caseSensitive);
        }
        internal SendTracing Tracing { get; }
        internal NatsSendOperation Rent()
        {
            for (var i = 0; i < _operations.Length; i++) if (Interlocked.Exchange(ref _operations[i], null) is { } operation) return operation;
            return new(this, _caseSensitive);
        }
        internal void Return(NatsSendOperation operation)
        {
            for (var i = 0; i < _operations.Length; i++) if (Interlocked.CompareExchange(ref _operations[i], operation, null) is null) return;
        }
    }
}
