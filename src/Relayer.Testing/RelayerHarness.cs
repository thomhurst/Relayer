using Relayer.Transports;
using Relayer.Pipeline;
using System.Buffers;
using Relayer.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Testing;

/// <summary>Provider-owned message observations and deterministic tracking of active work.</summary>
/// <remarks>Snapshots allocate intentionally. Register only in tests. One instance observes every named transport in its provider.</remarks>
public sealed class RelayerHarness : ISendMiddleware, IRelayerObserver
{
    private readonly IServiceProvider? _services;
    private readonly MessageTypeRegistry? _messageTypes;

    /// <summary>Creates standalone work tracking. Typed reads require AddRelayerHarness registration.</summary>
    public RelayerHarness() { }

    internal RelayerHarness(IServiceProvider services, MessageTypeRegistry messageTypes)
    {
        _services = services;
        _messageTypes = messageTypes;
    }

    private readonly object _gate = new();
    private readonly List<HarnessMessage> _sent = [];
    private readonly List<HarnessMessage> _dispatched = [];
    private readonly List<HarnessMessage> _consumed = [];
    private readonly List<HarnessFailure> _failed = [];
    private readonly List<HarnessDeadLetter> _deadLettered = [];
    private readonly Dictionary<string, HarnessMessage> _identities = new(StringComparer.Ordinal);
    private TaskCompletionSource _idle = CompletedSignal();
    private int _work;

    /// <summary>Successful logical sends, including outbox enlistments and publish/subscribe routes.</summary>
    public IReadOnlyList<HarnessMessage> Sent { get { lock (_gate) return _sent.ToArray(); } }
    /// <summary>Successful native sends, including direct outbox dispatch through registered transports.</summary>
    public IReadOnlyList<HarnessMessage> Dispatched { get { lock (_gate) return _dispatched.ToArray(); } }
    /// <summary>Deliveries presented to the consume pipeline, including failed deliveries and redeliveries.</summary>
    public IReadOnlyList<HarnessMessage> Consumed { get { lock (_gate) return _consumed.ToArray(); } }
    /// <summary>Failed sends, dispatches and consume attempts.</summary>
    public IReadOnlyList<HarnessFailure> Failed { get { lock (_gate) return _failed.ToArray(); } }
    /// <summary>Successful native dead-letter operations.</summary>
    public IReadOnlyList<HarnessDeadLetter> DeadLettered { get { lock (_gate) return _deadLettered.ToArray(); } }

    /// <summary>Reads successful logical sends of the registered message identity.</summary>
    public IReadOnlyList<TMessage> GetSent<TMessage>() => ReadMessages<TMessage>(Sent);

    /// <summary>Reads successful native sends of the registered message identity.</summary>
    public IReadOnlyList<TMessage> GetDispatched<TMessage>() => ReadMessages<TMessage>(Dispatched);

    /// <summary>Reads consume attempts of the registered message identity, including redeliveries.</summary>
    public IReadOnlyList<TMessage> GetConsumed<TMessage>() => ReadMessages<TMessage>(Consumed);

    /// <summary>Reads an observation using this provider's serializer for its named transport.</summary>
    /// <remarks>The observation must have the requested registered wire identity. Logical sends return their original object.</remarks>
    public TMessage Read<TMessage>(HarnessMessage observation)
    {
        ArgumentNullException.ThrowIfNull(observation);
        var identity = GetIdentity<TMessage>();
        if (!string.Equals(observation.WireType, identity, StringComparison.Ordinal))
            throw new InvalidOperationException($"The observation has wire type '{observation.WireType}', not '{identity}'.");
        return ReadCore<TMessage>(observation);
    }

    private string GetIdentity<TMessage>()
    {
        if (_messageTypes is null)
            throw new InvalidOperationException("Resolve RelayerHarness from services registered with AddRelayerHarness to use typed reads.");
        if (!_messageTypes.TryGetMessageType(typeof(TMessage), out var identity))
            throw new InvalidOperationException($"Register message type '{typeof(TMessage)}' with AddMessageTypesFromThisAssembly or MessageTypes.Add before reading observations.");
        return identity.ToString();
    }

    private IReadOnlyList<TMessage> ReadMessages<TMessage>(IReadOnlyList<HarnessMessage> observations)
    {
        var identity = GetIdentity<TMessage>();
        var messages = new List<TMessage>();
        foreach (var observation in observations)
            if (string.Equals(observation.WireType, identity, StringComparison.Ordinal)
                && observation.Matches<TMessage>())
                messages.Add(ReadCore<TMessage>(observation));
        return messages.ToArray();
    }

    private TMessage ReadCore<TMessage>(HarnessMessage observation)
    {
        if (observation.Message is TMessage message) return message;
        if (observation.Message is not null)
            throw new InvalidOperationException($"The logical observation contains '{observation.Message.GetType()}', not '{typeof(TMessage)}'.");
        var serializer = observation.TransportName is { } name
            ? _services!.GetMessageSerializer(name)
            : _services!.GetRequiredService<IMessageSerializer>();
        return observation.Read<TMessage>(serializer);
    }

    /// <summary>Waits for observed operations and queued in-memory messages to finish, without polling.</summary>
    /// <remarks>Start producers first. Future external arrivals are not observable. Use TrackWork across external producer or outbox scheduling gaps.</remarks>
    public Task WaitForIdleAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate) return _idle.Task.WaitAsync(cancellationToken);
    }

    /// <summary>Includes application-owned asynchronous work, such as an outbox dispatch cycle, in idle detection.</summary>
    public IDisposable TrackWork()
    {
        WorkStarted();
        return new WorkLease(this);
    }

    /// <summary>Clears recorded observations after idle. Does not purge native queues, broker state or durable outboxes.</summary>
    public void Reset()
    {
        lock (_gate)
        {
            if (_work != 0) throw new InvalidOperationException("Wait for harness idle before calling Reset.");
            _sent.Clear();
            _dispatched.Clear();
            _consumed.Clear();
            _failed.Clear();
            _deadLettered.Clear();
            _identities.Clear();
        }
    }

    async ValueTask ISendMiddleware.InvokeAsync(SendContext context, SendDelegate next)
    {
        WorkStarted();
        try
        {
            var operation = (ISendOperation)context;
            var message = new HarnessMessage(context.MessageId, context.MessageType.ToString(), default,
                operation.TransportName, operation.Destination, message: context.Message, usesOutbox: operation.UsesOutbox,
                isTombstone: context.Message is ITombstone, tombstoneKey: (context.Message as ITombstone)?.Key);
            lock (_gate) _identities[context.MessageId] = message;
            try
            {
                await next(context).ConfigureAwait(false);
                lock (_gate) _sent.Add(message);
            }
            catch (Exception error)
            {
                RecordFailure(message, error, HarnessOperation.Send);
                throw;
            }
        }
        finally { WorkCompleted(); }
    }

    async ValueTask IRelayerObserver.ProcessAsync(DefaultMessageContext context, ConsumeDelegate next)
    {
        WorkStarted();
        try
        {
            lock (_gate) _consumed.Add(Snapshot(context));
            await next(context).ConfigureAwait(false);
        }
        finally { WorkCompleted(); }
    }

    async ValueTask IRelayerObserver.ProcessBatchAsync(ConsumePipeline owner,
        ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken)
    {
        WorkStarted();
        try
        {
            lock (_gate)
                foreach (var context in contexts.Span) _consumed.Add(Snapshot(context));
            await owner.ProcessBatchCoreAsync(contexts, cancellationToken).ConfigureAwait(false);
        }
        finally { WorkCompleted(); }
    }

    ConsumeDelegate IRelayerObserver.WrapSettlement(ConsumeDelegate next) => async context =>
    {
        var delivery = (DefaultMessageContext)context;
        var disposition = delivery.ObservedDisposition;
        await next(context).ConfigureAwait(false);
        // The in-memory adapter observes actual settlement, including manual native settlement.
        if (disposition.Kind == DispositionKind.DeadLetter && context.Features.Get<NativeMessage<InMemoryDelivery>>() is null)
            RecordDeadLetter(Snapshot(delivery), disposition.DeadLetter);
    };

    void IRelayerObserver.Failed(DefaultMessageContext context, Exception error) => RecordFailure(Snapshot(context), error, HarnessOperation.Consume);

    void IRelayerObserver.DeadLettered(InMemoryDelivery delivery, DeadLetterInfo error)
    {
        var names = new HeaderNames();
        InMemoryMessageBinding.TryGetHeader(delivery, names.Id, out var id);
        InMemoryMessageBinding.TryGetHeader(delivery, names.Type, out var type);
        RecordDeadLetter(Correlate(id.IsEmpty ? null : id.ToString(), type.IsEmpty ? null : type.ToString(),
            delivery.Message.Body.ToArray(), delivery.Attempt), error);
    }

    private HarnessMessage Snapshot(DefaultMessageContext context) => Correlate(context.GetLogMessageId(),
        context.GetLogMessageType(), context.Body.ToArray(), context.Attempt,
        isTombstone: context.IsTombstone, tombstoneKey: context.TombstoneKey);

    internal HarnessMessage Correlate(string? id, string? wireType, ReadOnlyMemory<byte> body, int attempt = 1,
        string? transportName = null, string? destination = null, bool isTombstone = false, WireString? tombstoneKey = null)
    {
        lock (_gate)
        {
            var original = id is not null && _identities.TryGetValue(id, out var known) ? known : null;
            return new(id, wireType, body, transportName ?? original?.TransportName,
                destination ?? original?.Destination, attempt, isTombstone: isTombstone, tombstoneKey: tombstoneKey);
        }
    }

    internal void RecordDispatch(HarnessMessage message)
    {
        lock (_gate) _dispatched.Add(message);
    }

    internal void Remember(HarnessMessage message)
    {
        lock (_gate)
            if (message.MessageId is { } id) _identities[id] = message;
    }

    internal void RecordFailure(HarnessMessage message, Exception error, HarnessOperation operation)
    {
        lock (_gate) _failed.Add(new(message, error, operation));
    }

    private void RecordDeadLetter(HarnessMessage message, DeadLetterInfo error)
    {
        var headers = new KeyValuePair<string, WireString>[error.Headers.Length];
        for (var i = 0; i < headers.Length; i++) headers[i] = new(error.Headers.Span[i].Key, new WireString(error.Headers.Span[i].Value.ToString()));
        lock (_gate) _deadLettered.Add(new(message, new(error.Reason, error.Description, headers)));
    }

    void IRelayerObserver.WorkStarted() => WorkStarted();
    void IRelayerObserver.WorkCompleted() => WorkCompleted();

    internal void WorkStarted()
    {
        lock (_gate)
            if (_work++ == 0) _idle = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    internal void WorkCompleted()
    {
        lock (_gate)
            if (--_work == 0) _idle.TrySetResult();
    }

    private static TaskCompletionSource CompletedSignal()
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        signal.SetResult();
        return signal;
    }

    private sealed class WorkLease(RelayerHarness owner) : IDisposable
    {
        private RelayerHarness? _owner = owner;
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.WorkCompleted();
    }
}
