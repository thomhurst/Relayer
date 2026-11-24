using System.Buffers;
using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Relayer.Transports;

namespace Relayer.GooglePubSub;

internal sealed class GooglePubSubTransport : ITransport, ITransportSendTracing, IGooglePubSubClients
{
    private readonly GooglePubSubTransportOptions _options;
    private readonly FrozenDictionary<string, Endpoint> _topics;
    private readonly FrozenDictionary<string, Subscription> _subscriptions;
    private readonly MessageTracing _tracing;
    private readonly bool _publisherOrderingEnabled;
    private bool _disposed;

    internal List<GooglePubSubConsumer> Consumers
    {
        get;
    } = [];

    internal GooglePubSubTransport(string name, GooglePubSubTransportOptions options, TransportDefinition definition, MessageTracing tracing)
    {
        Name = name;
        _tracing = tracing;

        if (options.Publisher is null || options.Subscriber is null)
        {
            throw new InvalidOperationException($"Pub/Sub transport '{name}': supply prepared native Publisher and Subscriber API clients. Clients and their channels remain caller-owned.");
        }

        ArgumentNullException.ThrowIfNull(options.PublisherSettings);
        _options = new()
        {
            Publisher = options.Publisher,
            Subscriber = options.Subscriber,
            PublisherSettings = options.PublisherSettings.Clone()
        };
        _publisherOrderingEnabled = _options.PublisherSettings.EnableMessageOrdering;

        foreach (var topic in options.Topics)
        {
            _options.Topics.Add(topic);
        }

        var subscriptions = new Dictionary<string, Subscription>(options.Subscriptions.Count, StringComparer.Ordinal);

        for (var i = 0; i < options.Subscriptions.Count; i++)
        {
            var subscription = options.Subscriptions[i];

            if (!SubscriptionName.TryParse(subscription.Name, out var subscriptionName))
            {
                throw new ArgumentException($"Pub/Sub transport '{name}': Subscriptions[{i}].Name is '{subscription.Name}'. Use 'projects/{{project}}/subscriptions/{{subscription}}'.");
            }

            subscriptions.Add(subscriptionName.ToString(), subscription.Clone());
        }

        _subscriptions = subscriptions.ToFrozenDictionary(StringComparer.Ordinal);
        var topicsWithUnorderedSubscriptions = _subscriptions.Values
            .Where(static subscription => !subscription.EnableMessageOrdering)
            .Select(static subscription => subscription.Topic)
            .ToHashSet(StringComparer.Ordinal);
        var topics = new Dictionary<string, Endpoint>(StringComparer.Ordinal);

        foreach (var topic in options.Topics)
        {
            var destination = topic.ToString();

            if (topics.ContainsKey(destination))
            {
                throw new InvalidOperationException($"Pub/Sub transport '{name}': duplicate topic '{destination}'.");
            }

            var sendTracing = new SendTracing(tracing, new()
            {
                System = "gcp_pubsub",
                Destination = destination
            }, SendCoreAsync, SendBatchCoreAsync, headerNames: new("ce-"));
            var publisherApi = new GooglePubSubPublishTelemetry(options.Publisher, sendTracing);
            topics.Add(destination, new(new PublisherClientImpl(topic, [publisherApi], options.PublisherSettings.Clone(), static () => Task.CompletedTask),
                new()
                {
                    OrderedByKey = _publisherOrderingEnabled && !topicsWithUnorderedSubscriptions.Contains(destination),
                    MaxBatchSize = 1000,
                    MaxMessageBytes = 10_000_000
                }, sendTracing));
        }

        _topics = topics.ToFrozenDictionary(StringComparer.Ordinal);

        foreach (var subscription in _subscriptions.Values)
        {
            GetEndpoint(subscription.Topic);
            GooglePubSubTopology.ValidateDeclaration(subscription);
        }

        foreach (var route in definition.Routes)
        {
            GetEndpoint(route.Destination);
        }

        foreach (var declaration in definition.Consumers)
        {
            var consumer = (ConsumerDefinition<GooglePubSubConsumerOptions>)declaration;
            ArgumentNullException.ThrowIfNull(consumer.Options.Settings);
            ArgumentNullException.ThrowIfNull(consumer.Failure);
            var subscription = GetSubscription(consumer.Source);

            if (consumer.BatchSize != 1)
            {
                throw new NotSupportedException($"Pub/Sub consumer '{consumer.Source}': native streaming callbacks deliver individual messages. Use single-message handlers.");
            }

            if (consumer.Concurrency < 1 || consumer.Options.Settings.FlowControlSettings?.MaxOutstandingElementCount != consumer.Concurrency)
            {
                throw new InvalidOperationException($"Pub/Sub consumer '{consumer.Source}': set native Settings.FlowControlSettings.MaxOutstandingElementCount equal to Concurrency({consumer.Concurrency}). Native flow control owns the concurrency bound.");
            }

            if (consumer.Failure.RedeliveryDelays.Length != 0)
            {
                throw new NotSupportedException($"Pub/Sub consumer '{consumer.Source}': Nack cannot persist updated failure attributes. Use in-process RetryDelays and a DeadLetterEndpoint topic; durable redelivery is not supported.");
            }

            string? deadLetterEndpoint = consumer.Failure.DeadLetterEndpoint;

            if (string.IsNullOrWhiteSpace(deadLetterEndpoint))
            {
                throw new InvalidOperationException($"Pub/Sub consumer '{consumer.Source}': set Failure.DeadLetterEndpoint to a declared topic. Native DeadLetterPolicy cannot dead-letter on demand with error metadata.");
            }

            GetEndpoint(deadLetterEndpoint);

            if (subscription.Filter.Length == 0 && subscription.Topic == deadLetterEndpoint)
            {
                throw new InvalidOperationException($"Pub/Sub consumer '{consumer.Source}': the unfiltered subscription receives every message published to '{deadLetterEndpoint}', including its own dead-letter copies. Set Failure.DeadLetterEndpoint to a separate dead-letter topic.");
            }

            // Incoming keys can originate outside Relayer, even on unordered subscriptions.
            // Failure copies preserve them, and the native publisher rejects keys unless enabled.
            if (!_publisherOrderingEnabled)
            {
                throw new InvalidOperationException($"Pub/Sub consumer '{consumer.Source}': set PublisherSettings.EnableMessageOrdering = true so dead-letter copies can preserve incoming ordering keys.");
            }
        }
    }

    public string Name
    {
        get;
    }

    public PublisherServiceApiClient PublisherApi => _options.Publisher!;

    public SubscriberServiceApiClient SubscriberApi => _options.Subscriber!;

    public PublisherClient PublisherFor(string topic)
    {
        return GetEndpoint(topic).Publisher;
    }

    private Endpoint GetEndpoint(string name)
    {
        return _topics.TryGetValue(name, out var endpoint) ? endpoint
                : throw new InvalidOperationException($"Pub/Sub transport '{Name}': undeclared topic '{name}'. Use its fully qualified projects/.../topics/... name and declare it in Topics.");
    }

    internal string TopicFor(string source)
    {
        return GetSubscription(source).Topic;
    }

    private Subscription GetSubscription(string source)
    {
        return _subscriptions.TryGetValue(source, out var subscription) ? subscription
                : throw new InvalidOperationException($"Pub/Sub transport '{Name}': undeclared subscription '{source}'. Declare it in Subscriptions using its fully qualified name.");
    }

    public TransportCapabilities GetCapabilities(string endpoint)
    {
        return _subscriptions.TryGetValue(endpoint, out var subscription)
                ? new()
                {
                    OrderedByKey = subscription.EnableMessageOrdering,
                    MaxMessageBytes = 10_000_000
                }
                : GetEndpoint(endpoint).Capabilities;
    }

    public SendTracing? GetSendTracing(string destination)
    {
        return GetEndpoint(destination).Tracing;
    }

    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return GooglePubSubTopology.ApplyAsync(_options, _subscriptions.Values, mode, cancellationToken);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        return GetEndpoint(message.Destination).Tracing.SendAsync(message, cancellationToken);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendCoreAsync(OutgoingMessage message, CancellationToken cancellationToken)
    {
        var endpoint = Validate(message, cancellationToken);
        var state = endpoint.Rent();

        try
        {
            state.Prepare(message);
            cancellationToken.ThrowIfCancellationRequested();
            endpoint.Tracing.RecordNativeSend();
            // PublishAsync has no cancellation parameter and borrows the message until completion.
            // Never abandon that task with WaitAsync: it would release borrowed serializer storage too soon.
            await endpoint.Publisher.PublishAsync(state.Message).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();
        }
        finally
        {
            endpoint.Return(state);
        }
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        if (messages.IsEmpty)
        {
            return default;
        }

        var destination = messages.Span[0].Destination;

        for (var i = 1; i < messages.Length; i++)
        {
            if (messages.Span[i].Destination != destination)
            {
                return SendMixedAsync(messages, cancellationToken);
            }
        }

        return GetEndpoint(destination).Tracing.SendBatchAsync(messages, cancellationToken);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendMixedAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        for (var i = 0; i < messages.Length; i++)
        {
            Validate(messages.Span[i], cancellationToken);
        }

        var pending = ArrayPool<ValueTask>.Shared.Rent(messages.Length);
        var count = 0;
        Exception? admissionFailure = null;

        try
        {
            try
            {
                // A topic may need messages from later runs to reach its native batch threshold.
                var start = 0;

                while (start < messages.Length)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var destination = messages.Span[start].Destination;
                    var end = start + 1;

                    while (end < messages.Length && messages.Span[end].Destination == destination)
                    {
                        end++;
                    }

                    var operation = GetEndpoint(destination).Tracing
                        .SendBatchAsync(messages.Slice(start, end - start), cancellationToken);

                    if (operation.IsCompleted)
                    {
                        await operation.ConfigureAwait(false);
                    }
                    else
                    {
                        pending[count++] = operation;
                    }

                    start = end;
                }
            }
            catch (Exception error)
            {
                admissionFailure = error;
            }

            // Await all started runs before the caller can reuse borrowed message storage.
            Exception? failure = null;

            for (var i = 0; i < count; i++)
            {
                try
                {
                    await pending[i].ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    failure ??= error;
                }
            }

            if ((failure ?? admissionFailure) is { } firstFailure)
            {
                ExceptionDispatchInfo.Capture(firstFailure).Throw();
            }
        }
        finally
        {
            ArrayPool<ValueTask>.Shared.Return(pending, clearArray: true);
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendBatchCoreAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        for (var i = 0; i < messages.Length; i++)
        {
            Validate(messages.Span[i], cancellationToken);
        }

        var endpoint = GetEndpoint(messages.Span[0].Destination);
        var pending = ArrayPool<SendState>.Shared.Rent(messages.Length);
        var count = 0;
        Exception? failure = null;

        try
        {
            try
            {
                for (; count < messages.Length; count++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var state = endpoint.Rent();
                    pending[count] = state;

                    try
                    {
                        state.Prepare(messages.Span[count]);
                        cancellationToken.ThrowIfCancellationRequested();
                        endpoint.Tracing.RecordNativeSend();
                        state.Pending = endpoint.Publisher.PublishAsync(state.Message);
                    }
                    catch
                    {
                        count++;
                        throw;
                    }
                }
            }
            catch (Exception error)
            {
                failure = error;
            }
            // Drain every admitted native send, including when a later callback throws.
            for (var i = 0; i < count; i++)
            {
                if (pending[i].Pending is not { } task)
                {
                    continue;
                }

                try
                {
                    await task.ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    failure ??= error;
                }
            }

            if (failure is not null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
        finally
        {
            for (var i = 0; i < count; i++)
            {
                endpoint.Return(pending[i]);
            }

            ArrayPool<SendState>.Shared.Return(pending, clearArray: true);
        }
    }

    private Endpoint Validate(OutgoingMessage message, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        token.ThrowIfCancellationRequested();

        if (message.IsTombstone || message.DeliverAt is not null)
        {
            throw new NotSupportedException("Pub/Sub has no native tombstones or per-message scheduled sends. Use the durable outbox for scheduling.");
        }

        var endpoint = GetEndpoint(message.Destination);

        // Native publishing can preserve keys on dead-letter copies even when delivery is unordered.
        if (message.OrderingKey is not null && !_publisherOrderingEnabled)
        {
            throw new InvalidOperationException("Pub/Sub publishing with an ordering key requires PublisherSettings.EnableMessageOrdering = true.");
        }

        return endpoint;
    }

    internal GooglePubSubConsumer CreateConsumer(string source, SubscriberClient.Settings settings)
    {
        var prepared = settings.Clone();
        return new(() => new SubscriberClientImpl(SubscriptionName.Parse(source), [_options.Subscriber!], prepared, static () => Task.CompletedTask), _tracing, TopicFor(source), this, source);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        var startedCount = 0;

        try
        {
            for (; startedCount < Consumers.Count; startedCount++)
            {
                Consumers[startedCount].Start();
            }
        }
        catch (Exception startError) when (startedCount > 0)
        {
            try
            {
                // Await rollback even if the startup token has since been canceled.
                await StopConsumersAsync(Consumers.Take(startedCount), CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception stopError)
            {
                throw new AggregateException("Pub/Sub startup and subscriber rollback both failed.", startError, stopError);
            }

            throw;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return StopConsumersAsync(Consumers, cancellationToken);
    }

    private static async Task StopConsumersAsync(IEnumerable<GooglePubSubConsumer> consumers, CancellationToken cancellationToken)
    {
        var stopTasks = consumers.Select(worker => worker.StopAsync(cancellationToken)).ToArray();
        List<Exception>? errors = null;

        foreach (var stop in stopTasks)
        {
            try
            {
                await stop.ConfigureAwait(false);
            }
            catch (Exception error)
            {
                (errors ??= []).Add(error);
            }
        }

        if (errors is not null)
        {
            throw new AggregateException(errors);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        List<Exception>? errors = null;

        foreach (var worker in Consumers)
        {
            try
            {
                await worker.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception error)
            {
                (errors ??= []).Add(error);
            }
        }

        foreach (var endpoint in _topics.Values)
        {
            try
            {
                await endpoint.Publisher.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception error)
            {
                (errors ??= []).Add(error);
            }
        }

        if (errors is not null)
        {
            throw new AggregateException(errors);
        }
    }

    private sealed class Endpoint(PublisherClient publisher, TransportCapabilities capabilities, SendTracing tracing)
    {
        internal PublisherClient Publisher
        {
            get;
        } = publisher;

        internal TransportCapabilities Capabilities
        {
            get;
        } = capabilities;

        internal SendTracing Tracing
        {
            get;
        } = tracing;
        private readonly GooglePubSubPool<SendState> _states = new(static () => new());

        internal SendState Rent()
        {
            return _states.Rent();
        }

        internal void Return(SendState state)
        {
            state.Message.Data = ByteString.Empty;
            state.Message.Attributes.Clear();
            state.Message.OrderingKey = "";
            state.Message.MessageId = "";
            state.Message.PublishTime = null;
            state.Pending = null;
            _states.Return(state);
        }
    }

    private sealed class SendState
    {
        internal readonly PubsubMessage Message = new();
        internal Task<string>? Pending;

        internal void Prepare(OutgoingMessage outgoing)
        {
            Message.Data = UnsafeByteOperations.UnsafeWrap(outgoing.Body);
            Message.OrderingKey = outgoing.OrderingKey?.ToString() ?? "";
            var failure = false;

            foreach (var header in outgoing.Headers.Span)
            {
                if (header.Key == FailureHeaders.History)
                {
                    GooglePubSubFailureHeaders.Write(Message, header.Value.ToString());
                    failure = true;
                }
                else
                {
                    Message.Attributes[header.Key] = header.Value.ToString();
                }
            }

            outgoing.ApplyNative(Message);

            if (failure)
            {
                GooglePubSubFailureHeaders.Validate(Message);
            }
        }
    }
}
