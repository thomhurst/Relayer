using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.AzureServiceBus;

internal sealed class ServiceBusSendTransport : ITransport, ITransportSendTracing
{
    private readonly ServiceBusClient _client;
    private readonly bool _ownsClient;
    private readonly FrozenDictionary<string, Endpoint> _endpoints;
    private readonly ServiceBusTopology _topology;
    private int _disposed;

    internal static void ValidateDeclarations(string name, AzureServiceBusTransportOptions options, IEnumerable<string> destinations)
    {
        var entities = new HashSet<string>(StringComparer.Ordinal);
        foreach (var queue in options.Queues)
            if (!entities.Add(queue.Name)) throw new InvalidOperationException($"Service Bus transport '{name}': duplicate entity '{queue.Name}'.");
        foreach (var topic in options.Topics)
            if (!entities.Add(topic.Name)) throw new InvalidOperationException($"Service Bus transport '{name}': duplicate entity '{topic.Name}'.");
        foreach (var destination in destinations)
            if (!entities.Contains(destination)) throw new InvalidOperationException($"Service Bus transport '{name}': destination '{destination}' needs a native queue or topic declaration.");
        var subscriptions = new HashSet<(string, string)>();
        foreach (var subscription in options.Subscriptions)
        {
            if (!options.Topics.Any(topic => topic.Name == subscription.TopicName))
                throw new InvalidOperationException($"Service Bus transport '{name}': declare topic '{subscription.TopicName}' for subscription '{subscription.SubscriptionName}'.");
            if (!subscriptions.Add((subscription.TopicName, subscription.SubscriptionName)))
                throw new InvalidOperationException($"Service Bus transport '{name}': duplicate subscription '{subscription.TopicName}/Subscriptions/{subscription.SubscriptionName}'.");
        }
    }

    internal ServiceBusSendTransport(string name, ServiceBusClient client, bool ownsClient,
        AzureServiceBusTransportOptions options, IEnumerable<string> destinations, TopologyMode mode,
        ServiceBusAdministrationClient? administration = null, MessageTracing? tracing = null, ILoggerFactory? logging = null)
    {
        Name = name;
        _client = client;
        _ownsClient = ownsClient;
        _topology = new(name, client, options, administration);
        var endpoints = new Dictionary<string, Endpoint>(StringComparer.Ordinal);
        foreach (var destination in destinations)
        {
            if (endpoints.ContainsKey(destination)) continue;
            var queue = options.Queues.SingleOrDefault(item => item.Name == destination);
            var topic = options.Topics.SingleOrDefault(item => item.Name == destination);
            if ((queue is null) == (topic is null))
                throw new InvalidOperationException($"Service Bus transport '{name}': destination '{destination}' needs exactly one native queue or topic declaration.");
            // Only Provision verifies settings before sends start. Link attachment cannot establish session requirements.
            var ordered = mode == TopologyMode.Provision && (queue?.RequiresSession == true ||
                (topic is not null && options.Subscriptions.Any(item => item.TopicName == destination) &&
                 options.Subscriptions.Where(item => item.TopicName == destination).All(item => item.RequiresSession)));
            var endpoint = new Endpoint(client.CreateSender(destination), new()
            {
                OrderedByKey = ordered,
                MaxBatchSize = int.MaxValue,
                MaxDeliveryDelay = DateTimeOffset.MaxValue - DateTimeOffset.UtcNow,
                // Receive features and namespace-dependent transaction/size limits are deliberately unresolved.
            });
            if (tracing is not null)
                endpoint.Tracing = new(tracing, new() { System = "servicebus", Destination = destination, ServerAddress = client.FullyQualifiedNamespace },
                    SendCoreAsync, SendBatchCoreAsync, logging?.CreateLogger<ServiceBusSendTransport>(), new("cloudEvents:"));
            endpoints.Add(destination, endpoint);
        }
        _endpoints = endpoints.ToFrozenDictionary(StringComparer.Ordinal);
    }

    public string Name { get; }
    private Endpoint GetEndpoint(string destination) => _endpoints.TryGetValue(destination, out var endpoint) ? endpoint
        : throw new InvalidOperationException($"Service Bus transport '{Name}': undeclared send destination '{destination}'.");

    public TransportCapabilities GetCapabilities(string endpoint) => GetEndpoint(endpoint).Capabilities;
    public SendTracing? GetSendTracing(string destination) => GetEndpoint(destination).Tracing;

    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _topology.ApplyAsync(mode, cancellationToken);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        GetEndpoint(message.Destination).Tracing is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage outgoing, CancellationToken cancellationToken)
    {
        Validate(outgoing, cancellationToken);
        var endpoint = GetEndpoint(outgoing.Destination);
        var state = endpoint.Messages.Rent();
        try
        {
            state.Prepare(outgoing);
            endpoint.Tracing?.RecordNativeSend();
            var duration = endpoint.Tracing?.StartNativeSend() ?? default;
            var failed = true;
            try
            {
                await endpoint.Sender.SendMessageAsync(state.Message, cancellationToken).ConfigureAwait(false);
                failed = false;
            }
            finally { duration.Complete(failed); }
        }
        finally { endpoint.Return(state); }
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.IsEmpty) return SendBatchCoreAsync(messages, cancellationToken);
        var destination = messages.Span[0].Destination;
        for (var i = 1; i < messages.Length; i++)
            if (messages.Span[i].Destination != destination) return SendMixedBatchAsync(messages, cancellationToken);
        return GetEndpoint(destination).Tracing is { } tracing ? tracing.SendBatchAsync(messages, cancellationToken) : SendBatchCoreAsync(messages, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendMixedBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        for (var i = 0; i < messages.Length; i++) Validate(messages.Span[i], cancellationToken);
        var start = 0;
        while (start < messages.Length)
        {
            var end = start + 1;
            while (end < messages.Length && messages.Span[end].Destination == messages.Span[start].Destination) end++;
            await SendBatchAsync(messages.Slice(start, end - start), cancellationToken).ConfigureAwait(false);
            start = end;
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        for (var i = 0; i < messages.Length; i++) Validate(messages.Span[i], cancellationToken);
        var index = 0;
        ServiceBusSendState? pending = null;
        Endpoint? pendingEndpoint = null;
        try
        {
            while (index < messages.Length)
            {
                var destination = messages.Span[index].Destination;
                var endpoint = GetEndpoint(destination);
                var admitted = endpoint.Batches.Rent();
                try
                {
                    using var batch = await endpoint.Sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
                    string? session = null;
                    string? partition = null;
                    string? transactionPartition = null;
                    while (index < messages.Length && messages.Span[index].Destination == destination)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (pending is null)
                        {
                            pendingEndpoint = endpoint;
                            pending = endpoint.Messages.Rent();
                            pending.Prepare(messages.Span[index]);
                        }
                        // Service Bus requires each native batch to share session and partition IDs.
                        if (admitted.Count != 0 && (pending.Message.SessionId != session || pending.Message.PartitionKey != partition ||
                            pending.Message.TransactionPartitionKey != transactionPartition)) break;
                        if (!batch.TryAddMessage(pending.Message))
                        {
                            if (admitted.Count == 0)
                                throw new ServiceBusException($"Message for '{messages.Span[index].Destination}' exceeds the native batch byte limit ({batch.MaxSizeInBytes} bytes).", ServiceBusFailureReason.MessageSizeExceeded);
                            break;
                        }
                        session = pending.Message.SessionId;
                        partition = pending.Message.PartitionKey;
                        transactionPartition = pending.Message.TransactionPartitionKey;
                        admitted.Add(pending);
                        pending = null;
                        index++;
                    }
                    endpoint.Tracing?.RecordNativeSend(admitted.Count);
                    var duration = endpoint.Tracing?.StartNativeSend() ?? default;
                    var failed = true;
                    try
                    {
                        await endpoint.Sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
                        failed = false;
                    }
                    finally { duration.Complete(failed); }
                }
                finally
                {
                    // The SDK borrows body section arrays through batch disposal, not only TryAddMessage.
                    foreach (var state in admitted) endpoint.Return(state);
                    admitted.Clear();
                    endpoint.Batches.Return(admitted);
                }
            }
        }
        finally { if (pending is not null) pendingEndpoint!.Return(pending); }
    }

    private void Validate(OutgoingMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (message.IsTombstone) throw new NotSupportedException("Service Bus does not support native tombstones.");
        var endpoint = GetEndpoint(message.Destination);
        if (message.OrderingKey is not null && !endpoint.Capabilities.OrderedByKey)
            throw new InvalidOperationException($"Service Bus destination '{message.Destination}': session ordering is unresolved. Declare RequiresSession on the queue or every topic subscription and use Provision to verify it; Validate/None do not establish session requirements.");
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        List<Exception>? failures = null;
        foreach (var endpoint in _endpoints.Values)
        {
            try { await endpoint.Sender.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { (failures ??= []).Add(error); }
        }
        if (_ownsClient)
        {
            try { await _client.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { (failures ??= []).Add(error); }
        }
        if (failures is { Count: 1 }) ExceptionDispatchInfo.Capture(failures[0]).Throw();
        if (failures is { Count: > 1 }) throw new AggregateException(failures);
    }

    private sealed class Endpoint(ServiceBusSender sender, TransportCapabilities capabilities)
    {
        internal ServiceBusSender Sender { get; } = sender;
        internal TransportCapabilities Capabilities { get; } = capabilities;
        internal ServiceBusPool<ServiceBusSendState> Messages { get; } = new(static () => new());
        internal ServiceBusPool<List<ServiceBusSendState>> Batches { get; } = new(static () => []);
        internal SendTracing? Tracing { get; set; }
        internal void Return(ServiceBusSendState state) { state.Clear(); Messages.Return(state); }
    }
}
