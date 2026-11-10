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
    private bool _disposed;
    internal List<GooglePubSubConsumer> Consumers { get; } = [];

    internal GooglePubSubTransport(string name, GooglePubSubTransportOptions options, TransportDefinition definition, MessageTracing tracing)
    {
        Name = name;
        _tracing = tracing;
        if (options.Publisher is null || options.Subscriber is null)
            throw new InvalidOperationException($"Pub/Sub transport '{name}': supply prepared native Publisher and Subscriber API clients. Clients and their channels remain caller-owned.");
        ArgumentNullException.ThrowIfNull(options.PublisherSettings);
        _options = new() { Publisher = options.Publisher, Subscriber = options.Subscriber, PublisherSettings = options.PublisherSettings.Clone() };
        foreach (var topic in options.Topics) _options.Topics.Add(topic);
        _subscriptions = options.Subscriptions.ToFrozenDictionary(subscription => SubscriptionName.Parse(subscription.Name).ToString(), subscription => subscription.Clone(), StringComparer.Ordinal);
        var topics = new Dictionary<string, Endpoint>(StringComparer.Ordinal);
        foreach (var topic in options.Topics)
        {
            var destination = topic.ToString();
            if (topics.ContainsKey(destination)) throw new InvalidOperationException($"Pub/Sub transport '{name}': duplicate topic '{destination}'.");
            topics.Add(destination, new(new PublisherClientImpl(topic, [options.Publisher], options.PublisherSettings.Clone(), static () => Task.CompletedTask),
                new() { OrderedByKey = options.PublisherSettings.EnableMessageOrdering == true, MaxBatchSize = 1000, MaxMessageBytes = 10_000_000 },
                new(tracing, new() { System = "gcp_pubsub", Destination = destination }, SendCoreAsync, SendBatchCoreAsync, headerNames: new("ce-"))));
        }
        _topics = topics.ToFrozenDictionary(StringComparer.Ordinal);
        foreach (var subscription in _subscriptions.Values)
        {
            GetEndpoint(subscription.Topic);
            GooglePubSubTopology.ValidateDeclaration(subscription);
        }
        foreach (var route in definition.Routes) GetEndpoint(route.Destination);
        foreach (var declaration in definition.Consumers)
        {
            var consumer = (ConsumerDefinition<GooglePubSubConsumerOptions>)declaration;
            ArgumentNullException.ThrowIfNull(consumer.Options.Settings);
            ArgumentNullException.ThrowIfNull(consumer.Options.Failure);
            TopicFor(consumer.Source);
            if (consumer.BatchSize != 1)
                throw new NotSupportedException($"Pub/Sub consumer '{consumer.Source}': native streaming callbacks deliver individual messages. Use single-message handlers.");
            if (consumer.Concurrency < 1 || consumer.Options.Settings.FlowControlSettings?.MaxOutstandingElementCount != consumer.Concurrency)
                throw new InvalidOperationException($"Pub/Sub consumer '{consumer.Source}': set native Settings.FlowControlSettings.MaxOutstandingElementCount equal to Concurrency({consumer.Concurrency}). Native flow control owns the concurrency bound.");
            if (!consumer.Options.Failure.RedeliveryDelays.IsEmpty)
                throw new NotSupportedException($"Pub/Sub consumer '{consumer.Source}': Nack cannot persist updated failure attributes. Use in-process RetryDelays and a DeadLetterEndpoint topic; durable redelivery is not supported.");
            if (string.IsNullOrWhiteSpace(consumer.Options.Failure.DeadLetterEndpoint))
                throw new InvalidOperationException($"Pub/Sub consumer '{consumer.Source}': set Failure.DeadLetterEndpoint to a declared topic. Native DeadLetterPolicy cannot dead-letter on demand with error metadata.");
            GetEndpoint(consumer.Options.Failure.DeadLetterEndpoint);
            // Incoming keys can originate outside Relayer, even on unordered subscriptions.
            // Failure copies preserve them, and the native publisher rejects keys unless enabled.
            if (_options.PublisherSettings.EnableMessageOrdering != true)
                throw new InvalidOperationException($"Pub/Sub consumer '{consumer.Source}': set PublisherSettings.EnableMessageOrdering = true so dead-letter copies can preserve incoming ordering keys.");
        }
    }

    public string Name { get; }
    public PublisherServiceApiClient PublisherApi => _options.Publisher!;
    public SubscriberServiceApiClient SubscriberApi => _options.Subscriber!;
    public PublisherClient PublisherFor(string topic) => GetEndpoint(topic).Publisher;
    private Endpoint GetEndpoint(string name) => _topics.TryGetValue(name, out var endpoint) ? endpoint
        : throw new InvalidOperationException($"Pub/Sub transport '{Name}': undeclared topic '{name}'. Use its fully qualified projects/.../topics/... name and declare it in Topics.");
    internal string TopicFor(string source) => _subscriptions.TryGetValue(source, out var subscription) ? subscription.Topic
        : throw new InvalidOperationException($"Pub/Sub transport '{Name}': undeclared subscription '{source}'. Declare it in Subscriptions using its fully qualified name.");

    public TransportCapabilities GetCapabilities(string endpoint) => _subscriptions.TryGetValue(endpoint, out var subscription)
        ? new() { OrderedByKey = subscription.EnableMessageOrdering, MaxMessageBytes = 10_000_000 }
        : GetEndpoint(endpoint).Capabilities;
    public SendTracing? GetSendTracing(string destination) => GetEndpoint(destination).Tracing;
    public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return GooglePubSubTopology.ApplyAsync(_options, _subscriptions.Values, mode, cancellationToken);
    }

    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
        GetEndpoint(message.Destination).Tracing.SendAsync(message, cancellationToken);

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
            var duration = endpoint.Tracing.StartNativeSend();
            var failed = true;
            try
            {
                // PublishAsync has no cancellation parameter and borrows the message until completion.
                // Never abandon that task with WaitAsync: it would release borrowed serializer storage too soon.
                await endpoint.Publisher.PublishAsync(state.Message).ConfigureAwait(false);
                failed = false;
            }
            finally { duration.Complete(failed); }
            cancellationToken.ThrowIfCancellationRequested();
        }
        finally { endpoint.Return(state); }
    }

    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        if (messages.IsEmpty) return default;
        var destination = messages.Span[0].Destination;
        for (var i = 1; i < messages.Length; i++)
            if (messages.Span[i].Destination != destination) return SendMixedAsync(messages, cancellationToken);
        return GetEndpoint(destination).Tracing.SendBatchAsync(messages, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SendMixedAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
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
        for (var i = 0; i < messages.Length; i++) Validate(messages.Span[i], cancellationToken);
        var endpoint = GetEndpoint(messages.Span[0].Destination);
        var pending = System.Buffers.ArrayPool<SendState>.Shared.Rent(messages.Length);
        var count = 0;
        Exception? failure = null;
        var duration = endpoint.Tracing.StartNativeSend();
        var failed = true;
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
                    catch { count++; throw; }
                }
            }
            catch (Exception error) { failure = error; }
            // Drain every admitted native send, including when a later callback throws.
            for (var i = 0; i < count; i++)
            {
                if (pending[i].Pending is not { } task) continue;
                try { await task.ConfigureAwait(false); }
                catch (Exception error) { failure ??= error; }
            }
            if (failure is not null) ExceptionDispatchInfo.Capture(failure).Throw();
            failed = false;
            cancellationToken.ThrowIfCancellationRequested();
        }
        finally
        {
            duration.Complete(failed);
            for (var i = 0; i < count; i++) endpoint.Return(pending[i]);
            System.Buffers.ArrayPool<SendState>.Shared.Return(pending, clearArray: true);
        }
    }

    private Endpoint Validate(OutgoingMessage message, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        token.ThrowIfCancellationRequested();
        if (message.IsTombstone || message.DeliverAt is not null)
            throw new NotSupportedException("Pub/Sub has no native tombstones or per-message scheduled sends. Use the durable outbox for scheduling.");
        var endpoint = GetEndpoint(message.Destination);
        if (message.OrderingKey is not null && !endpoint.Capabilities.OrderedByKey)
            throw new InvalidOperationException("Pub/Sub ordering requires native PublisherSettings.EnableMessageOrdering and ordering-enabled subscriptions.");
        return endpoint;
    }

    internal GooglePubSubConsumer AddConsumer(string source, SubscriberClient.Settings settings)
    {
        var prepared = settings.Clone();
        var worker = new GooglePubSubConsumer(() => new SubscriberClientImpl(SubscriptionName.Parse(source), [_options.Subscriber!], prepared, static () => Task.CompletedTask), _tracing, TopicFor(source), this, source);
        Consumers.Add(worker);
        return worker;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        foreach (var worker in Consumers) worker.Start();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        List<Exception>? errors = null;
        foreach (var worker in Consumers)
            try { await worker.StopAsync(cancellationToken).ConfigureAwait(false); }
            catch (Exception error) { (errors ??= []).Add(error); }
        if (errors is not null) throw new AggregateException(errors);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        List<Exception>? errors = null;
        foreach (var worker in Consumers)
            try { await worker.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { (errors ??= []).Add(error); }
        foreach (var endpoint in _topics.Values)
            try { await endpoint.Publisher.DisposeAsync().ConfigureAwait(false); }
            catch (Exception error) { (errors ??= []).Add(error); }
        if (errors is not null) throw new AggregateException(errors);
    }

    private sealed class Endpoint(PublisherClient publisher, TransportCapabilities capabilities, SendTracing tracing)
    {
        internal PublisherClient Publisher { get; } = publisher;
        internal TransportCapabilities Capabilities { get; } = capabilities;
        internal SendTracing Tracing { get; } = tracing;
        private readonly GooglePubSubPool<SendState> _states = new(static () => new());
        internal SendState Rent() => _states.Rent();
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
                else Message.Attributes[header.Key] = header.Value.ToString();
            }
            outgoing.ApplyNative(Message);
            if (failure) GooglePubSubFailureHeaders.Validate(Message);
        }
    }
}
