using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer.AzureServiceBus;

internal sealed partial class ServiceBusSendTransport : ITransport, ITransportSendTracing
{
    private readonly ServiceBusClient _client;
    private readonly bool _ownsClient;
    private readonly ILogger? _logger;
    private readonly FrozenDictionary<string, Endpoint> _endpoints;
    private readonly ServiceBusTopology _topology;
    private readonly ServiceBusTransportOptions _declaredOptions;
    private int _disposed;
    private readonly HashSet<string> _receiveSources;
    private readonly IReadOnlyList<IConsumerDefinition> _declarations;
    private readonly List<ServiceBusReceiver> _healthReceivers = [];

    internal ServiceBusClient Client => _client;

    internal List<ServiceBusConsumer> Consumers
    {
        get;
    } = [];

    internal bool HealthTracking
    {
        get; set;
    }

    internal static void ValidateDeclarations(string name, ServiceBusTransportOptions options, IEnumerable<string> destinations)
    {
        var entities = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var queue in options.Queues)
        {
            if (!entities.Add(queue.Name))
            {
                throw new InvalidOperationException($"Service Bus transport '{name}': duplicate entity '{queue.Name}'. Queue and topic names are case-insensitive.");
            }
        }

        foreach (var topic in options.Topics)
        {
            if (!entities.Add(topic.Name))
            {
                throw new InvalidOperationException($"Service Bus transport '{name}': duplicate entity '{topic.Name}'. Queue and topic names are case-insensitive.");
            }
        }

        foreach (var destination in destinations)
        {
            if (!entities.Contains(destination))
            {
                throw new InvalidOperationException($"Service Bus transport '{name}': destination '{destination}' needs a native queue or topic declaration.");
            }
        }

        foreach (var entity in options.TrustedSessionEntities)
        {
            if (options.Topics.Any(topic => topic.Name == entity))
            {
                if (options.Subscriptions.FirstOrDefault(subscription => subscription.TopicName == entity && !subscription.RequiresSession) is { } plain)
                {
                    throw new InvalidOperationException($"Service Bus transport '{name}': TrustedSessionEntities entry '{entity}' asserts that every subscription requires sessions, but subscription '{entity}/Subscriptions/{plain.SubscriptionName}' is declared with RequiresSession=false.");
                }

                continue;
            }

            if (!options.Queues.Any(queue => queue.Name == entity && queue.RequiresSession) &&
                !options.Subscriptions.Any(subscription => subscription.TopicName + "/Subscriptions/" + subscription.SubscriptionName == entity && subscription.RequiresSession))
            {
                throw new InvalidOperationException($"Service Bus transport '{name}': TrustedSessionEntities entry '{entity}' needs a native topic declaration, or a native queue or subscription declaration with RequiresSession=true.");
            }
        }

        var subscriptions = new HashSet<(string, string)>();

        foreach (var subscription in options.Subscriptions)
        {
            if (!options.Topics.Any(topic => topic.Name == subscription.TopicName))
            {
                throw new InvalidOperationException($"Service Bus transport '{name}': declare topic '{subscription.TopicName}' for subscription '{subscription.SubscriptionName}'.");
            }

            if (!subscriptions.Add((subscription.TopicName, subscription.SubscriptionName)))
            {
                throw new InvalidOperationException($"Service Bus transport '{name}': duplicate subscription '{subscription.TopicName}/Subscriptions/{subscription.SubscriptionName}'.");
            }
        }
    }

    internal ServiceBusSendTransport(string name, ServiceBusClient client, bool ownsClient,
        ServiceBusTransportOptions options, IEnumerable<string> destinations, TopologyMode mode,
        ServiceBusAdministrationClient? administration = null, MessageTracing? tracing = null, ILoggerFactory? logging = null, IReadOnlyList<IConsumerDefinition>? consumers = null,
        IReadOnlyList<IRouteDefinition>? routes = null, MessageTypeRegistry? types = null,
        ServiceBusTransportOptions? declaredOptions = null)
    {
        Name = name;
        _client = client;
        _ownsClient = ownsClient;
        _declaredOptions = declaredOptions ?? options;
        _logger = logging?.CreateLogger<ServiceBusSendTransport>();
        _topology = new(name, client, options, administration, consumers, destinations.ToArray());
        _declarations = consumers ?? [];
        _receiveSources = consumers is null ? [] : consumers.Select(consumer => consumer.Source).ToHashSet(StringComparer.Ordinal);
        var endpoints = new Dictionary<string, Endpoint>(StringComparer.Ordinal);

        foreach (var destination in destinations)
        {
            if (endpoints.ContainsKey(destination))
            {
                continue;
            }

            var queue = options.Queues.SingleOrDefault(item => string.Equals(item.Name, destination, StringComparison.OrdinalIgnoreCase));
            var topic = options.Topics.SingleOrDefault(item => string.Equals(item.Name, destination, StringComparison.OrdinalIgnoreCase));

            if ((queue is null) == (topic is null))
            {
                throw new InvalidOperationException($"Service Bus transport '{name}': destination '{destination}' needs exactly one native queue or topic declaration.");
            }
            // Link attachment cannot establish session requirements. Trust is explicit per native entity.
            var entityName = queue?.Name ?? topic!.Name;
            var ordered = queue is not null
                ? queue.RequiresSession && (mode == TopologyMode.Provision || options.TrustedSessionEntities.Contains(entityName))
                : options.TrustedSessionEntities.Contains(entityName) ||
                  options.Subscriptions.Any(item => item.TopicName == entityName) &&
                  options.Subscriptions.Where(item => item.TopicName == entityName).All(item => item.RequiresSession &&
                      (mode == TopologyMode.Provision || options.TrustedSessionEntities.Contains(item.TopicName + "/Subscriptions/" + item.SubscriptionName)));
            var endpoint = new Endpoint(client.CreateSender(destination), SubjectHeaders(options, destination, routes), IdHeaders(destination, routes),
                RouteCallbacks(name, destination, routes, types), new()
                {
                    OrderedByKey = ordered,
                    MaxBatchSize = int.MaxValue,
                    MaxDeliveryDelay = DateTimeOffset.MaxValue - DateTimeOffset.UtcNow,
                    // Receive features and namespace-dependent transaction/size limits are deliberately unresolved.
                });

            if (tracing is not null)
            {
                endpoint.Tracing = new(tracing, new()
                {
                    System = "servicebus",
                    Destination = destination,
                    ServerAddress = client.FullyQualifiedNamespace
                },
                    SendCoreAsync, SendBatchCoreAsync, _logger, new("cloudEvents:"));
            }

            endpoints.Add(destination, endpoint);
        }

        _endpoints = endpoints.ToFrozenDictionary(StringComparer.Ordinal);
    }

    internal void ValidateRedeliveryBudget(string source, string? subscription, string entity, int nativeRedeliveries)
    {
        // An implied declaration has SDK defaults, which say nothing about the broker's actual limit.
        int? limit = subscription is null
            ? _declaredOptions.Queues.SingleOrDefault(queue => queue.Name == source)?.MaxDeliveryCount
            : _declaredOptions.Subscriptions.SingleOrDefault(item => item.TopicName == source && item.SubscriptionName == subscription)?.MaxDeliveryCount;

        if (limit <= nativeRedeliveries)
        {
            throw new InvalidOperationException($"Service Bus consumer '{entity}' on transport '{Name}': declared MaxDeliveryCount is {limit}. Set MaxDeliveryCount to at least {nativeRedeliveries + 1} for the initial delivery and {nativeRedeliveries} native redeliveries, or reduce RedeliveryDelays.");
        }
    }

    // The CloudEvents type header names of the routes to a destination; their value becomes the native Subject.
    private static string[] SubjectHeaders(ServiceBusTransportOptions options, string destination, IReadOnlyList<IRouteDefinition>? routes)
    {
        if (!options.SubjectFromMessageType)
        {
            return [];
        }

        if (routes is null)
        {
            return [new HeaderNames("cloudEvents:").Type];
        }

        return routes.Where(route => route.Destination == destination && route.SendOptions.EnvelopeMode == MessageEnvelopeMode.CloudEvents)
            .Select(route => route.SendOptions.HeaderNames.Type).Distinct(StringComparer.Ordinal).ToArray();
    }

    // The CloudEvents id header names of the routes to a destination; their value becomes the native MessageId. The default
    // binding stays included for copies sent without a route, such as dead-letter and scheduled copies.
    private static string[] IdHeaders(string destination, IReadOnlyList<IRouteDefinition>? routes)
    {
        var standard = new HeaderNames("cloudEvents:").Id;

        if (routes is null)
        {
            return [standard];
        }

        return routes.Where(route => route.Destination == destination && route.SendOptions.EnvelopeMode == MessageEnvelopeMode.CloudEvents)
            .Select(route => route.SendOptions.HeaderNames.Id).Append(standard).Distinct(StringComparer.Ordinal).ToArray();
    }

    // Route-level callbacks for a destination, matched to each send by the route's CloudEvents type header (#808).
    private static ServiceBusRouteCallback[] RouteCallbacks(string name, string destination,
        IReadOnlyList<IRouteDefinition>? routes, MessageTypeRegistry? types)
    {
        if (routes is null)
        {
            return [];
        }

        List<ServiceBusRouteCallback>? callbacks = null;

        foreach (var route in routes)
        {
            if (route.Destination != destination || route is not ISendRouteDefinition { NativeConfigurator: { } callback })
            {
                continue;
            }

            var location = $"Service Bus transport '{name}': route for '{route.MessageType}' to '{destination}'";

            if (callback is not Action<OutgoingMessage, ServiceBusMessage> configure)
            {
                throw new InvalidOperationException($"{location}: the native callback is not a Service Bus route callback.");
            }

            if (route.SendOptions.EnvelopeMode != MessageEnvelopeMode.CloudEvents)
            {
                throw new InvalidOperationException($"{location}: a native route callback needs the CloudEvents envelope, whose type header identifies the route when stored outbox rows are dispatched.");
            }

            if (types is null || !types.TryGetMessageType(route.MessageType, out var type))
            {
                throw new InvalidOperationException($"{location}: register a stable message identity before declaring a native route callback.");
            }

            (callbacks ??= []).Add(new(route.SendOptions.HeaderNames.Type, type.ToString(), configure));
        }

        return callbacks?.ToArray() ?? [];
    }

    public string Name
    {
        get;
    }

    private Endpoint GetEndpoint(string destination)
    {
        return _endpoints.TryGetValue(destination, out var endpoint) ? endpoint
                : throw new InvalidOperationException($"Service Bus transport '{Name}': undeclared send destination '{destination}'.");
    }

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        if (_receiveSources.Contains(endpoint))
        {
            if (_endpoints.TryGetValue(endpoint, out var send))
            {
                return send.Capabilities with
                {
                    DeadLetter = true,
                    MaxRedeliveryDelay = TimeSpan.Zero
                };
            }

            return new()
            {
                DeadLetter = true,
                MaxRedeliveryDelay = TimeSpan.Zero
            };
        }

        return GetEndpoint(endpoint).Capabilities;
    }

    public SendTracing? GetSendTracing(string destination)
    {
        return GetEndpoint(destination).Tracing;
    }

    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _topology.ApplyAsync(mode, cancellationToken);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        return GetEndpoint(message.Destination).Tracing is { } tracing ? tracing.SendAsync(message, cancellationToken) : SendCoreAsync(message, cancellationToken);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage outgoing, CancellationToken cancellationToken)
    {
        Validate(outgoing, cancellationToken);
        var endpoint = GetEndpoint(outgoing.Destination);
        var state = endpoint.Messages.Rent();

        try
        {
            state.Prepare(outgoing, endpoint.SubjectHeaders, endpoint.IdHeaders, endpoint.RouteCallbacks);
            cancellationToken.ThrowIfCancellationRequested();
            endpoint.Tracing?.RecordNativeSend();
            var duration = endpoint.Tracing?.StartNativeSend() ?? default;
            var failed = true;

            try
            {
                await endpoint.Sender.SendMessageAsync(state.Message, cancellationToken).ConfigureAwait(false);
                failed = false;
            }
            finally
            {
                duration.Complete(failed);
            }
        }
        finally
        {
            endpoint.Return(state);
        }
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        if (messages.IsEmpty)
        {
            return SendBatchCoreAsync(messages, cancellationToken);
        }

        var destination = messages.Span[0].Destination;

        for (var i = 1; i < messages.Length; i++)
        {
            if (messages.Span[i].Destination != destination)
            {
                return SendMixedBatchAsync(messages, cancellationToken);
            }
        }

        return GetEndpoint(destination).Tracing is { } tracing ? tracing.SendBatchAsync(messages, cancellationToken) : SendBatchCoreAsync(messages, cancellationToken);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendMixedBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        for (var i = 0; i < messages.Length; i++)
        {
            Validate(messages.Span[i], cancellationToken);
        }

        var start = 0;

        while (start < messages.Length)
        {
            var end = start + 1;

            while (end < messages.Length && messages.Span[end].Destination == messages.Span[start].Destination)
            {
                end++;
            }

            await SendBatchAsync(messages.Slice(start, end - start), cancellationToken).ConfigureAwait(false);
            start = end;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        for (var i = 0; i < messages.Length; i++)
        {
            Validate(messages.Span[i], cancellationToken);
        }

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
                            pending.Prepare(messages.Span[index], endpoint.SubjectHeaders, endpoint.IdHeaders, endpoint.RouteCallbacks);
                        }
                        // Service Bus requires each native batch to share session and partition IDs.
                        if (admitted.Count != 0 && (pending.Message.SessionId != session || pending.Message.PartitionKey != partition ||
                            pending.Message.TransactionPartitionKey != transactionPartition))
                        {
                            break;
                        }

                        if (!batch.TryAddMessage(pending.Message))
                        {
                            if (admitted.Count == 0)
                            {
                                throw new ServiceBusException($"Message for '{messages.Span[index].Destination}' exceeds the native batch byte limit ({batch.MaxSizeInBytes} bytes).", ServiceBusFailureReason.MessageSizeExceeded);
                            }

                            break;
                        }

                        session = pending.Message.SessionId;
                        partition = pending.Message.PartitionKey;
                        transactionPartition = pending.Message.TransactionPartitionKey;
                        admitted.Add(pending);
                        pending = null;
                        index++;
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    endpoint.Tracing?.RecordNativeSend(admitted.Count);
                    var duration = endpoint.Tracing?.StartNativeSend() ?? default;
                    var failed = true;

                    try
                    {
                        await endpoint.Sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
                        failed = false;
                    }
                    finally
                    {
                        duration.Complete(failed);
                    }
                }
                finally
                {
                    // The SDK borrows body section arrays through batch disposal, not only TryAddMessage.
                    foreach (var state in admitted)
                    {
                        endpoint.Return(state);
                    }

                    admitted.Clear();
                    endpoint.Batches.Return(admitted);
                }
            }
        }
        finally
        {
            if (pending is not null)
            {
                pendingEndpoint!.Return(pending);
            }
        }
    }

    private void Validate(OutgoingMessage message, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (message.IsTombstone)
        {
            throw new NotSupportedException("Service Bus does not support native tombstones.");
        }

        var endpoint = GetEndpoint(message.Destination);

        if (message.OrderingKey is not null && !endpoint.Capabilities.OrderedByKey)
        {
            throw new InvalidOperationException($"Service Bus destination '{message.Destination}': session ordering is unresolved. Declare RequiresSession on the queue or every topic subscription and use Provision to verify it, or add each entity to TrustedSessionEntities to trust its declaration in Validate/None. For a topic whose subscriptions other applications own, add the topic name to TrustedSessionEntities.");
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            foreach (var consumer in Consumers)
            {
                await consumer.StartAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            var rollback = StopAsync(CancellationToken.None);

            try
            {
                await rollback.ConfigureAwait(false);
            }
            catch (Exception error)
            {
                if (_logger is { } logger)
                {
                    StartupRollbackFailed(logger, rollback.Exception ?? error, Name);
                }
            }

            throw;
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Service Bus transport '{Transport}' could not stop all processors after startup failed.")]
    private static partial void StartupRollbackFailed(ILogger logger, Exception exception, string transport);

    public Task StopAsync(CancellationToken cancellationToken)
    {
        // Start every drain before awaiting it so one busy declaration cannot keep siblings accepting messages.
        return Task.WhenAll(Consumers.Select(consumer => consumer.StopAsync(cancellationToken)));
    }

    internal void EnableHealthTracking()
    {
        if (HealthTracking)
        {
            return;
        }

        HealthTracking = true;

        foreach (var declaration in _declarations)
        {
            var consumer = (ConsumerDefinition<ServiceBusConsumerOptions>)declaration;
            _healthReceivers.Add(consumer.Options.Subscription is { } subscription
                ? _client.CreateReceiver(consumer.Source, subscription) : _client.CreateReceiver(consumer.Source));
        }

        foreach (var consumer in Consumers)
        {
            consumer.TrackHealth = true;
        }
    }

    internal async ValueTask CheckConnectivityAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();

        foreach (var endpoint in _endpoints)
        {
            // The SDK caches negotiated batch limits. A fresh link is required for each Send-only probe;
            // CreateMessageBatchAsync on a previously opened sender can succeed without contacting the broker.
            var sender = _client.CreateSender(endpoint.Key);
            await using var lifetime = sender.ConfigureAwait(false);
            using var batch = await sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var receiver in _healthReceivers)
        {
            await receiver.PeekMessageAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        if (_endpoints.Count == 0 && _healthReceivers.Count == 0)
        {
            throw new InvalidOperationException($"Service Bus transport '{Name}': declare a send or receive endpoint for a native connectivity probe.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        List<Exception>? failures = null;

        foreach (var consumer in Consumers)
        {
            try
            {
                await consumer.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception error)
            {
                (failures ??= []).Add(error);
            }
        }

        foreach (var receiver in _healthReceivers)
        {
            try
            {
                await receiver.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception error)
            {
                (failures ??= []).Add(error);
            }
        }

        foreach (var endpoint in _endpoints.Values)
        {
            try
            {
                await endpoint.Sender.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception error)
            {
                (failures ??= []).Add(error);
            }
        }

        if (_ownsClient)
        {
            try
            {
                await _client.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception error)
            {
                (failures ??= []).Add(error);
            }
        }

        if (failures is { Count: 1 })
        {
            ExceptionDispatchInfo.Capture(failures[0]).Throw();
        }

        if (failures is { Count: > 1 })
        {
            throw new AggregateException(failures);
        }
    }

    private sealed class Endpoint(ServiceBusSender sender, string[] subjectHeaders, string[] idHeaders,
        ServiceBusRouteCallback[] routeCallbacks, TransportCapabilities capabilities)
    {
        internal ServiceBusSender Sender
        {
            get;
        } = sender;

        internal string[] SubjectHeaders
        {
            get;
        } = subjectHeaders;

        internal string[] IdHeaders
        {
            get;
        } = idHeaders;

        internal ServiceBusRouteCallback[] RouteCallbacks
        {
            get;
        } = routeCallbacks;

        internal TransportCapabilities Capabilities
        {
            get;
        } = capabilities;

        internal ServiceBusPool<ServiceBusSendState> Messages
        {
            get;
        } = new(static () => new());

        internal ServiceBusPool<List<ServiceBusSendState>> Batches
        {
            get;
        } = new(static () => []);

        internal SendTracing? Tracing
        {
            get; set;
        }

        internal void Return(ServiceBusSendState state)
        {
            state.Clear();
            Messages.Return(state);
        }
    }
}
