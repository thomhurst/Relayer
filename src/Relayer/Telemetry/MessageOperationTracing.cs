using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Relayer;

/// <summary>A precomputed native endpoint operation. Construct once at startup; Start supports concurrent calls.</summary>
public sealed class MessageOperationTracing
{
    private readonly MessageTracing _tracing;
    private readonly MessagingOperation _operation;
    private readonly string _name;
    private readonly KeyValuePair<string, object?>[] _tags;
    private readonly SamplingState?[] _states = new SamplingState[64];

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
        _states[0] = new(_tags);
    }

    /// <summary>Starts an activity, or returns null without tag work or allocations when no listener is attached.</summary>
    /// <remarks>
    /// Dispose the result after the native operation completes. Use its context as creation context only when the
    /// message has none. Batch send callers create per-message Create operations first unless disabled in TracingOptions.
    /// Existing creation contexts skip Create and make Send a client span. Invalid batch metadata is rejected even
    /// without a listener or when creation is skipped. No exception events are recorded.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Activity? Start(in MessageTraceData data = default)
        => Start(data, enrich: true);

    internal Activity? Start(in MessageTraceData data, bool enrich)
    {
        if (data.BatchCount is < 1) throw new ArgumentOutOfRangeException(nameof(data), "Batch count must be positive.");
        if (_operation == MessagingOperation.Create && data.BatchCount is not null)
            throw new ArgumentException("A create span describes one message, never a batch.", nameof(data));
        if (!_tracing.Source.HasListeners()) return null;
        if (_operation == MessagingOperation.Create && data.CreationContext != default) return null;
        return StartActivity(data, enrich);
    }

    private Activity? StartActivity(in MessageTraceData data, bool enrich)
    {
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
        var hasLinks = _operation is MessagingOperation.Send or MessagingOperation.Process &&
            (!data.Links.IsEmpty || data.CreationContext != default);
        var hasPartition = data.BatchCount is null && data.PartitionId is not null;
        SamplingState? state = null;
        try
        {
            IEnumerable<KeyValuePair<string, object?>> tags = _tags;
            IEnumerable<ActivityLink>? links = null;
            if (hasPartition || hasLinks)
            {
                state = Rent();
                if (hasPartition)
                {
                    state.Tags[^1] = new("messaging.destination.partition.id", data.PartitionId);
                    tags = state.Tags;
                }
                if (hasLinks)
                {
                    if (data.Links.IsEmpty) state.Links.Add(new ActivityLink(data.CreationContext));
                    else
                        foreach (var link in data.Links.Span) state.Links.Add(link);
                    links = state.Links;
                }
            }
            // ActivitySource consumes tags and links synchronously, including sampling and its own copies.
            var activity = _tracing.Source.StartActivity(_name, kind, parent, tags, links);
            if (activity is { IsAllDataRequested: true })
            {
                if (data.BatchCount is { } count) activity.SetTag("messaging.batch.message_count", count);
                else
                {
                    activity.SetTag("messaging.message.id", data.MessageId);
                    activity.SetTag("messaging.message.conversation_id", data.ConversationId);
                    activity.SetTag("cloudevents.event_type", data.EventType);
                    activity.SetTag("cloudevents.event_source", data.EventSource);
                    if (_tracing.Options.IncludeBodySize && data.BodySize is { } size) activity.SetTag("messaging.message.body.size", size);
                }
            }
            if (enrich && activity is not null) _tracing.Enrich(activity);
            return activity;
        }
        finally
        {
            if (state is not null) Return(state);
        }
    }

    /// <summary>Wraps one actual native settlement call with a settle span. Cache the returned delegate at startup.</summary>
    /// <remarks>
    /// Use the same wrapper for core and manual settlement. Select the native operation before invoking it;
    /// skip it for SettledByHandler. The wrapper does not change dispositions or implement settlement.
    /// </remarks>
    public ConsumeDelegate WrapSettlement(ConsumeDelegate settlement)
    {
        ArgumentNullException.ThrowIfNull(settlement);
        if (_operation != MessagingOperation.Settle)
            throw new InvalidOperationException("WrapSettlement requires a settle operation with the native operation name.");
        return new Settlement(this, settlement).InvokeAsync;
    }

    private sealed class Settlement(MessageOperationTracing operation, ConsumeDelegate settlement)
    {
        internal ValueTask InvokeAsync(MessageContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            if (operation._tracing.Source.HasListeners()) return InvokeTracedAsync(context);
            try
            {
                var pending = settlement(context);
                if (!pending.IsCompletedSuccessfully) return AwaitUntracedAsync(context, pending);
                // Consume a completed ValueTask without creating an async state machine.
                pending.GetAwaiter().GetResult();
                return default;
            }
            catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                (context as DefaultMessageContext)?.LogSettlementFailure(error);
                throw;
            }
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        private static async ValueTask AwaitUntracedAsync(MessageContext context, ValueTask pending)
        {
            try { await pending.ConfigureAwait(false); }
            catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                (context as DefaultMessageContext)?.LogSettlementFailure(error);
                throw;
            }
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        private async ValueTask InvokeTracedAsync(MessageContext context)
        {
            var native = context as DefaultMessageContext;
            using var activity = native is not null
                ? ConsumeTracing.Start(operation._tracing, operation, native, linkCreation: false)
                : operation.Start();
            try
            {
                await settlement(context).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
            catch (Exception error)
            {
                Fail(activity, "_OTHER");
                native?.LogSettlementFailure(error);
                throw;
            }
        }
    }

    internal KeyValuePair<string, object?>[] CreateBatchTags()
    {
        var tags = new KeyValuePair<string, object?>[_tags.Length + 1];
        _tags.CopyTo(tags, 0);
        return tags;
    }

    internal Activity? StartBatch(KeyValuePair<string, object?>[] tags, List<ActivityLink> links)
    {
        var activity = _tracing.Source.StartActivity(_name, ActivityKind.Consumer, Activity.Current?.Context ?? default, tags, links);
        if (activity is not null) _tracing.Enrich(activity);
        return activity;
    }

    private SamplingState Rent()
    {
        for (var i = 0; i < _states.Length; i++)
            if (Interlocked.Exchange(ref _states[i], null) is { } state) return state;
        return new(_tags);
    }

    private void Return(SamplingState state)
    {
        state.Tags[^1] = default;
        state.Links.Clear();
        for (var i = 0; i < _states.Length; i++)
            if (Interlocked.CompareExchange(ref _states[i], state, null) is null) return;
    }

    private sealed class SamplingState
    {
        internal KeyValuePair<string, object?>[] Tags { get; }
        internal List<ActivityLink> Links { get; } = new(4);

        internal SamplingState(KeyValuePair<string, object?>[] tags)
        {
            Tags = new KeyValuePair<string, object?>[tags.Length + 1];
            tags.CopyTo(Tags, 0);
        }
    }

    /// <summary>Marks failure without reflection or exception events. The caller logs the exception with trace IDs.</summary>
    /// <remarks>Missing or whitespace error types use _OTHER so diagnostics never mask the original failure.</remarks>
    public static void Fail(Activity? activity, string errorType)
    {
        if (activity is not { IsAllDataRequested: true }) return;
        activity.SetTag("error.type", string.IsNullOrWhiteSpace(errorType) ? "_OTHER" : errorType);
        activity.SetStatus(ActivityStatusCode.Error);
    }

    private static void Add(ActivityTagsCollection tags, string name, string? value)
    {
        if (value is not null) tags.Add(name, value);
    }
}
