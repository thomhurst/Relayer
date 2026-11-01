using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using Dekaf.Producer;
using Dekaf.Serialization;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.Kafka;

internal sealed class KafkaProducerTransport : ITransport, ITransportSendTracing
{
    private readonly IKafkaProducer<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)> _producer;
    private readonly FrozenDictionary<string, SendTracing?> _endpoints;
    private readonly KafkaSendPool _operations = new();
    private bool _disposed;
    private static readonly TransportCapabilities Capabilities = new()
    {
        OrderedByKey = true,
        MaxBatchSize = int.MaxValue,
        ConsumptionModel = ConsumptionModel.Log,
    };

    internal KafkaProducerTransport(string name, IKafkaProducer<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)> producer,
        IEnumerable<string> destinations, MessageTracing? tracing = null, ILoggerFactory? loggerFactory = null)
    {
        Name = name;
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
    }

    public string Name { get; }
    public TransportCapabilities GetCapabilities(string endpoint) { Endpoint(endpoint); return Capabilities; }
    public SendTracing? GetSendTracing(string destination) => Endpoint(destination);

    private SendTracing? Endpoint(string destination) => _endpoints.TryGetValue(destination, out var tracing) ? tracing
        : throw new InvalidOperationException($"Kafka transport '{Name}': undeclared topic '{destination}'. Declare a send route first.");

    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (mode != TopologyMode.None)
            throw new NotSupportedException($"Kafka transport '{Name}': topology validation and provisioning are not implemented. Set TopologyMode.None and manage topics externally.");
        return new(new TopologyReport([]));
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        Endpoint(message.Destination) is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        Validate(message, cancellationToken);
        var operation = _operations.Rent(1);
        try
        {
            operation.States[0].Prepare(message);
            await _producer.ProduceAsync(message.Destination, operation.States[0].Key, (message.Body, operation.States[0].Headers), cancellationToken).ConfigureAwait(false);
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
            for (var i = 0; i < messages.Length; i++)
            {
                operation.States[i].Prepare(messages.Span[i]);
                operation.Messages.Add((operation.States[i].Key, (messages.Span[i].Body, operation.States[i].Headers)));
            }
            // Delegate batching, admission, ordering and partial failures to Dekaf. Its aggregate
            // awaits every admitted operation before completion, including cancellation and errors.
            await _producer.ProduceAllAsync(messages.Span[0].Destination, operation.Messages, cancellationToken).ConfigureAwait(false);
        }
        finally { _operations.Return(operation); }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendMixedBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        for (var i = 0; i < messages.Length; i++) Validate(messages.Span[i], cancellationToken);
        for (var i = 0; i < messages.Length; i++)
            await SendAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
    }

    private void Validate(OutgoingMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        Endpoint(message.Destination);
        if (message.DeliverAt is not null)
            throw new NotSupportedException("Kafka has no native scheduled send. Use a durable scheduler before dispatch.");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _producer.InitializeAsync(cancellationToken).ConfigureAwait(false);
    }
    public async Task StopAsync(CancellationToken cancellationToken) =>
        await _producer.FlushAsync(cancellationToken).ConfigureAwait(false);
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _producer.DisposeAsync().ConfigureAwait(false);
    }

}
