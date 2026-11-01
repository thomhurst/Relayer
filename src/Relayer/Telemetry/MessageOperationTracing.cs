using System.Diagnostics;

namespace Relayer;

/// <summary>A precomputed native endpoint operation. Construct once at startup; Start supports concurrent calls.</summary>
public sealed class MessageOperationTracing
{
    private readonly MessageTracing _tracing;
    private readonly MessagingOperation _operation;
    private readonly string _name;
    private readonly KeyValuePair<string, object?>[] _tags;

    /// <summary>Precomputes the span name and constant sampling attributes.</summary>
    /// <param name="tracing">The application's source owner.</param>
    /// <param name="endpoint">Native endpoint metadata.</param>
    /// <param name="operation">One of Relayer's supported operation types.</param>
    /// <param name="nativeOperation">Required for settlement (ack, commit, complete, etc.); optional for send (schedule).</param>
    public MessageOperationTracing(MessageTracing tracing, MessagingEndpoint endpoint, MessagingOperation operation,
        string? nativeOperation = null)
    {
        ArgumentNullException.ThrowIfNull(tracing);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint.System);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint.Destination);
        var type = operation switch
        {
            MessagingOperation.Create => "create",
            MessagingOperation.Send => "send",
            MessagingOperation.Process => "process",
            MessagingOperation.Settle => "settle",
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };
        if (operation == MessagingOperation.Settle) ArgumentException.ThrowIfNullOrWhiteSpace(nativeOperation);
        if (nativeOperation is not null && operation is not (MessagingOperation.Send or MessagingOperation.Settle))
            throw new ArgumentException("Only send and settle operations have native operation names.", nameof(nativeOperation));
        if (nativeOperation is not null) ArgumentException.ThrowIfNullOrWhiteSpace(nativeOperation);
        _tracing = tracing;
        _operation = operation;
        var operationName = nativeOperation ?? type;
        var destination = endpoint.DestinationTemplate ?? (endpoint.Temporary || endpoint.Anonymous ? null : endpoint.Destination);
        _name = destination is null ? operationName : $"{operationName} {destination}";
        var tags = new ActivityTagsCollection
        {
            { "messaging.system", endpoint.System },
            { "messaging.operation.name", operationName },
            { "messaging.operation.type", type },
            { "messaging.destination.name", endpoint.Destination },
        };
        Add(tags, "messaging.destination.template", endpoint.DestinationTemplate);
        if (endpoint.Temporary) tags.Add("messaging.destination.temporary", true);
        if (endpoint.Anonymous) tags.Add("messaging.destination.anonymous", true);
        Add(tags, "messaging.client.id", endpoint.ClientId);
        Add(tags, "server.address", endpoint.ServerAddress);
        if (endpoint.ServerPort is { } port) tags.Add("server.port", port);
        if (operation == MessagingOperation.Process)
        {
            Add(tags, "messaging.consumer.group.name", endpoint.ConsumerGroup);
            Add(tags, "messaging.destination.subscription.name", endpoint.Subscription);
        }
        _tags = tags.ToArray();
    }

    /// <summary>Starts an activity, or returns null without tag work or allocations when no listener is attached.</summary>
    /// <remarks>
    /// Dispose the result after the native operation completes. Use its context as creation context only when the
    /// message has none. Batch send callers create per-message Create operations first unless disabled in TracingOptions.
    /// Existing creation contexts skip Create and make Send a client span. No exception events are recorded.
    /// </remarks>
    public Activity? Start(in MessageTraceData data = default)
    {
        if (!_tracing.Source.HasListeners()) return null;
        if (_operation == MessagingOperation.Create && data.CreationContext != default) return null;
        if (data.BatchCount is < 1) throw new ArgumentOutOfRangeException(nameof(data), "Batch count must be positive.");
        if (_operation == MessagingOperation.Create && data.BatchCount is not null)
            throw new ArgumentException("A create span describes one message, never a batch.", nameof(data));
        var tags = new ActivityTagsCollection(_tags);
        if (data.BatchCount is { } count) tags.Add("messaging.batch.message_count", count);
        else
        {
            Add(tags, "messaging.message.id", data.MessageId);
            Add(tags, "messaging.message.conversation_id", data.ConversationId);
            Add(tags, "messaging.destination.partition.id", data.PartitionId);
            Add(tags, "cloudevents.event_type", data.EventType);
            Add(tags, "cloudevents.event_source", data.EventSource);
            if (_tracing.Options.IncludeBodySize && data.BodySize is { } size) tags.Add("messaging.message.body.size", size);
        }
        var kind = _operation switch
        {
            MessagingOperation.Create => ActivityKind.Producer,
            MessagingOperation.Process => ActivityKind.Consumer,
            MessagingOperation.Settle => ActivityKind.Client,
            _ => data.CreationContext != default || !data.Links.IsEmpty ? ActivityKind.Client : ActivityKind.Producer,
        };
        var parent = Activity.Current?.Context ?? default;
        if (_operation == MessagingOperation.Process && parent == default && data.BatchCount is null &&
            _tracing.Options.CreationContextAsParent) parent = data.CreationContext;
        IEnumerable<ActivityLink>? links = null;
        if (_operation is MessagingOperation.Send or MessagingOperation.Process)
        {
            if (!data.Links.IsEmpty) links = data.Links.ToArray();
            else if (data.CreationContext != default) links = new[] { new ActivityLink(data.CreationContext) };
        }
        var activity = _tracing.Source.StartActivity(_name, kind, parent, tags, links);
        if (activity is not null) _tracing.Enrich(activity);
        return activity;
    }

    /// <summary>Marks failure without reflection or exception events. The caller logs the exception with trace IDs.</summary>
    public static void Fail(Activity? activity, string errorType)
    {
        if (activity is not { IsAllDataRequested: true }) return;
        ArgumentException.ThrowIfNullOrWhiteSpace(errorType);
        activity.SetTag("error.type", errorType);
        activity.SetStatus(ActivityStatusCode.Error);
    }

    private static void Add(ActivityTagsCollection tags, string name, string? value)
    {
        if (value is not null) tags.Add(name, value);
    }
}
