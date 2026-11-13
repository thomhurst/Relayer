using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using Dekaf.Admin;
using Dekaf.Producer;
using Dekaf.Serialization;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Kafka;

internal sealed class KafkaProducerTransport : ITransport, ITransportSendTracing
{
    private readonly IKafkaProducer<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?> _producer;
    private readonly FrozenDictionary<string, SendTracing?> _endpoints;
    private readonly KafkaSendPool _operations = new();
    private readonly FrozenDictionary<string, ITopicProducer<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>> _topics;
    private readonly KafkaTransportOptions? _options;
    private bool _disposed;
    internal List<KafkaConsumer> Consumers { get; } = [];
    private static readonly TransportCapabilities Capabilities = new()
    {
        OrderedByKey = true,
        Tombstones = true,
        MaxBatchSize = int.MaxValue,
        ConsumptionModel = ConsumptionModel.Log,
    };

    internal KafkaProducerTransport(string name, IKafkaProducer<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?> producer,
        IEnumerable<string> destinations, MessageTracing? tracing = null, ILoggerFactory? loggerFactory = null, KafkaTransportOptions? options = null)
    {
        Name = name;
        _options = options;
        _producer = producer;
        var endpoints = new Dictionary<string, SendTracing?>(StringComparer.Ordinal);
        foreach (var destination in destinations)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(destination);
            endpoints.TryAdd(destination, tracing is null ? null : new(tracing,
                new() { System = "kafka", Destination = destination }, SendCoreAsync, SendBatchCoreAsync,
                loggerFactory?.CreateLogger<KafkaProducerTransport>()));
        }
        _endpoints = endpoints.ToFrozenDictionary(StringComparer.Ordinal);
        _topics = endpoints.Keys.ToFrozenDictionary(static topic => topic, topic => producer.ForTopic(topic), StringComparer.Ordinal);
    }

    public string Name { get; }
    public TransportCapabilities GetCapabilities(string endpoint) { Endpoint(endpoint); return Capabilities; }
    public SendTracing? GetSendTracing(string destination) => Endpoint(destination);

    private SendTracing? Endpoint(string destination) => _endpoints.TryGetValue(destination, out var tracing) ? tracing
        : throw new InvalidOperationException($"Kafka transport '{Name}': undeclared topic '{destination}'. Declare a send route first.");

    public async ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (mode == TopologyMode.None) return new TopologyReport([]);
        if (mode != TopologyMode.Validate)
            throw new NotSupportedException($"Kafka transport '{Name}': topology provisioning is not implemented. Create topics externally and use TopologyMode.Validate or TopologyMode.None.");
        if (_endpoints.Count == 0) return new TopologyReport([]);
        var options = _options ?? throw new InvalidOperationException("Kafka topology validation requires native admin configuration.");
        var builder = new AdminClientBuilder().WithBootstrapServers(options.BootstrapServers.ToArray());
        options.ConfigureAdmin?.Invoke(builder);
        await using var admin = builder.Build();
        // Listing all topics never requests metadata for a missing name: unlike targeted
        // DescribeTopicsAsync in Dekaf 1.27, it cannot trigger broker auto-creation.
        var topics = await admin.ListTopicsAsync(new ListTopicsOptions { ListInternal = true }, cancellationToken).ConfigureAwait(false);
        var existing = new HashSet<string>(topics.Select(static topic => topic.Name), StringComparer.Ordinal);
        var differences = new List<string>();
        foreach (var topic in _endpoints.Keys.Order(StringComparer.Ordinal))
            if (!existing.Contains(topic))
                differences.Add($"Topic '{topic}' is missing or not visible to the admin client. Create it externally and grant topic metadata access (ConfigureAdmin).");
        return new TopologyReport(differences);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        Endpoint(message.Destination) is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var tracing = Validate(message, cancellationToken);
        var operation = _operations.Rent(1);
        try
        {
            operation.States[0].Prepare(message);
            var state = operation.States[0];
            tracing?.RecordNativeSend();
            var duration = tracing?.StartNativeSend() ?? default;
            var failed = true;
            try
            {
                if (state.Tombstone)
                    await _topics[message.Destination].ProduceAsync(state.Key, null, state.Headers, cancellationToken).ConfigureAwait(false);
                else
                    await _producer.ProduceAsync(message.Destination, state.Key, (message.Body, state.Headers), cancellationToken).ConfigureAwait(false);
                failed = false;
            }
            finally { duration.Complete(failed); }
        }
        finally { _operations.Return(operation); }
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.IsEmpty) return SendBatchCoreAsync(messages, cancellationToken);
        var destination = messages.Span[0].Destination;
        for (var i = 1; i < messages.Length; i++)
            if (messages.Span[i].Destination != destination) return SendMixedBatchAsync(messages, cancellationToken);
        return Endpoint(destination) is { } tracing ? tracing.SendBatchAsync(messages, cancellationToken) : SendBatchCoreAsync(messages, cancellationToken);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        for (var i = 0; i < messages.Length; i++) Validate(messages.Span[i], cancellationToken);
        if (messages.IsEmpty) return;
        var operation = _operations.Rent(messages.Length);
        try
        {
            var hasTombstone = false;
            for (var i = 0; i < messages.Length; i++)
            {
                operation.States[i].Prepare(messages.Span[i]);
                hasTombstone |= operation.States[i].Tombstone;
                operation.Messages.Add((operation.States[i].Key, (messages.Span[i].Body, operation.States[i].Headers)));
            }
            // The tuple batch overload cannot carry headers alongside a null value. Admit
            // null-containing batches through native sends and drain every admitted operation
            // before returning borrowed storage, including after cancellation or failure.
            if (hasTombstone)
                await SendNullBatchAsync(messages, operation, cancellationToken).ConfigureAwait(false);
            else
            {
                var tracing = Endpoint(messages.Span[0].Destination);
                tracing?.RecordNativeSend(messages.Length);
                var duration = tracing?.StartNativeSend() ?? default;
                var failed = true;
                try
                {
                    await _producer.ProduceAllAsync(messages.Span[0].Destination, operation.Messages, cancellationToken).ConfigureAwait(false);
                    failed = false;
                }
                finally { duration.Complete(failed); }
            }
        }
        finally { _operations.Return(operation); }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendNullBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, KafkaSendOperation operation, CancellationToken token)
    {
        operation.EnsurePending(messages.Length);
        var tracing = Endpoint(messages.Span[0].Destination);
        var count = 0;
        System.Runtime.ExceptionServices.ExceptionDispatchInfo? failure = null;
        try
        {
            for (; count < messages.Length; count++)
            {
                var message = messages.Span[count];
                var state = operation.States[count];
                tracing?.RecordNativeSend();
                operation.Pending[count] = ProduceAsync(message, state, token);
            }
        }
        catch (Exception error) { failure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error); }
        for (var i = 0; i < count; i++)
        {
            try { await operation.Pending[i].ConfigureAwait(false); }
            catch (Exception error) { failure ??= System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error); }
        }
        failure?.Throw();
    }

    private ValueTask<RecordMetadata> ProduceAsync(OutgoingMessage message, KafkaSendState state, CancellationToken token)
    {
        var duration = Endpoint(message.Destination)?.StartNativeSend() ?? default;
        try
        {
            // Preserve synchronous admission failures: stop admitting and drain earlier calls.
            var pending = state.Tombstone
                ? _topics[message.Destination].ProduceAsync(state.Key, null, state.Headers, token)
                : _producer.ProduceAsync(message.Destination, state.Key, (message.Body, state.Headers), token);
            return duration.IsEnabled ? state.TrackSend(pending, duration) : pending;
        }
        catch
        {
            duration.Complete(failed: true);
            throw;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendMixedBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        for (var i = 0; i < messages.Length; i++) Validate(messages.Span[i], cancellationToken);
        for (var i = 0; i < messages.Length; i++)
            await SendAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
    }

    private SendTracing? Validate(OutgoingMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        var tracing = Endpoint(message.Destination);
        if (message.DeliverAt is not null)
            throw new NotSupportedException("Kafka has no native scheduled send. Use a durable scheduler before dispatch.");
        return tracing;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        try
        {
            await _producer.InitializeAsync(cancellationToken).ConfigureAwait(false);
            foreach (var consumer in Consumers) await consumer.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal every receiver before awaiting any failure or a slow batch drain.
        try { await Task.WhenAll(Consumers.Select(consumer => consumer.StopAsync(cancellationToken))).ConfigureAwait(false); }
        finally { await _producer.FlushAsync(cancellationToken).ConfigureAwait(false); }
    }
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        try { foreach (var consumer in Consumers) await consumer.DisposeAsync().ConfigureAwait(false); }
        finally { await _producer.DisposeAsync().ConfigureAwait(false); }
    }

}
