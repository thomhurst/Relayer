using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Testing;

/// <summary>Provider-owned message observations and deterministic tracking of active work.</summary>
/// <remarks>Snapshots allocate intentionally. Register only in tests. One instance observes every named transport in its provider.</remarks>
public sealed class RelayerHarness : ISendMiddleware, IRelayerObserver
{
    private readonly IServiceProvider? _services;
    private readonly MessageTypeRegistry? _messageTypes;

    /// <summary>Creates standalone work tracking. Typed reads require AddRelayerHarness registration.</summary>
    public RelayerHarness()
    {
    }

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
    private readonly List<HarnessMessage> _handled = [];
    private readonly Dictionary<DefaultMessageContext, int> _failing = new(ReferenceEqualityComparer.Instance);
    private readonly List<IConsumedWaiter> _waiters = [];
    private TaskCompletionSource _idle = CompletedSignal();
    private int _work;
    private readonly Dictionary<Guid, int> _outboxRows = [];

    /// <summary>Successful logical sends, including outbox enlistments and publish/subscribe routes.</summary>
    public IReadOnlyList<HarnessMessage> Sent
    {
        get
        {
            lock (_gate)
            {
                return _sent.ToArray();
            }
        }
    }

    /// <summary>Successful native sends, including direct outbox dispatch through registered transports.</summary>
    public IReadOnlyList<HarnessMessage> Dispatched
    {
        get
        {
            lock (_gate)
            {
                return _dispatched.ToArray();
            }
        }
    }

    /// <summary>Deliveries presented to the consume pipeline, including failed deliveries and redeliveries.</summary>
    public IReadOnlyList<HarnessMessage> Consumed
    {
        get
        {
            lock (_gate)
            {
                return _consumed.ToArray();
            }
        }
    }

    /// <summary>Failures observed by send middleware, native dispatch and consume instrumentation.</summary>
    /// <remarks>Route and destination selection errors occur before send middleware and are reported to the caller.</remarks>
    public IReadOnlyList<HarnessFailure> Failed
    {
        get
        {
            lock (_gate)
            {
                return _failed.ToArray();
            }
        }
    }

    /// <summary>Successful native dead-letter operations.</summary>
    public IReadOnlyList<HarnessDeadLetter> DeadLettered
    {
        get
        {
            lock (_gate)
            {
                return _deadLettered.ToArray();
            }
        }
    }

    /// <summary>Reads successful logical sends of the registered message identity.</summary>
    public IReadOnlyList<TMessage> GetSent<TMessage>()
    {
        return ReadMessages<TMessage>(Sent);
    }

    /// <summary>Reads successful native sends of the registered message identity.</summary>
    public IReadOnlyList<TMessage> GetDispatched<TMessage>()
    {
        return ReadMessages<TMessage>(Dispatched);
    }

    /// <summary>Reads consume attempts of the registered message identity, including redeliveries.</summary>
    public IReadOnlyList<TMessage> GetConsumed<TMessage>()
    {
        return ReadMessages<TMessage>(Consumed);
    }

    /// <summary>Reads an observation using this provider's serializer for its named transport.</summary>
    /// <remarks>The observation must have the requested registered wire identity. Logical sends return their original object.</remarks>
    public TMessage Read<TMessage>(HarnessMessage observation)
    {
        ArgumentNullException.ThrowIfNull(observation);
        var identity = GetIdentity<TMessage>();

        if (!string.Equals(observation.WireType, identity, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"The observation has wire type '{observation.WireType}', not '{identity}'.");
        }

        return ReadCore<TMessage>(observation);
    }

    private string GetIdentity<TMessage>()
    {
        if (_messageTypes is null)
        {
            throw new InvalidOperationException("Resolve RelayerHarness from services registered with AddRelayerHarness to use typed reads.");
        }

        if (!_messageTypes.TryGetMessageType(typeof(TMessage), out var identity))
        {
            throw new InvalidOperationException($"Register message type '{typeof(TMessage)}' with AddMessageTypesFromThisAssembly or MessageTypes.Add before reading observations.");
        }

        return identity.ToString();
    }

    private IReadOnlyList<TMessage> ReadMessages<TMessage>(IReadOnlyList<HarnessMessage> observations)
    {
        var identity = GetIdentity<TMessage>();
        var messages = new List<TMessage>();

        foreach (var observation in observations)
        {
            if (string.Equals(observation.WireType, identity, StringComparison.Ordinal)
                && observation.Matches<TMessage>())
            {
                messages.Add(ReadCore<TMessage>(observation));
            }
        }

        return messages.ToArray();
    }

    private TMessage ReadCore<TMessage>(HarnessMessage observation)
    {
        if (observation.Message is TMessage message)
        {
            return message;
        }

        if (observation.Message is not null)
        {
            throw new InvalidOperationException($"The logical observation contains '{observation.Message.GetType()}', not '{typeof(TMessage)}'.");
        }

        var serializer = observation.TransportName is { } name
            ? _services!.GetMessageSerializer(name)
            : _services!.GetRequiredService<IMessageSerializer>();
        return observation.Read<TMessage>(serializer);
    }

    /// <summary>Waits for observed operations, queued in-memory messages and committed outbox rows to finish, without polling.</summary>
    /// <remarks>Start producers first. Future external arrivals are not observable. A committed outbox row counts until this
    /// provider's dispatcher sends, discards or poisons it; a retried row stays outstanding. Use TrackWork across external producer gaps.</remarks>
    public Task WaitForIdleAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return _idle.Task.WaitAsync(cancellationToken);
        }
    }

    /// <summary>Waits for the first matching successful delivery in harness recording order, without polling.</summary>
    /// <remarks>Includes deliveries handled before the call. Failed attempts and dead-lettered deliveries do not match; a later successful redelivery does.
    /// Use this for broker round-trips, where idle cannot be inferred. Each wait evaluates its predicate sequentially.
    /// The predicate can run on the calling or consuming thread and must not block.</remarks>
    public async Task<TMessage> WaitForConsumedAsync<TMessage>(Func<TMessage, bool>? predicate = null, CancellationToken cancellationToken = default)
    {
        var waiter = new ConsumedWaiter<TMessage>(this, GetIdentity<TMessage>(), predicate);
        using var cancellation = cancellationToken.UnsafeRegister(
            static (state, token) => ((ConsumedWaiter<TMessage>)state!).TrySetCanceled(token), waiter);
        HarnessMessage[] handled;

        lock (_gate)
        {
            handled = _handled.ToArray();
            _waiters.Add(waiter);
        }

        try
        {
            foreach (var observation in handled)
            {
                if (waiter.TryComplete(observation))
                {
                    break;
                }
            }

            if (!waiter.Task.IsCompleted)
            {
                Drain(waiter);
            }

            return await waiter.Task.ConfigureAwait(false);
        }
        finally
        {
            lock (_gate)
            {
                _waiters.Remove(waiter);
                waiter.ClearPending();
            }
        }
    }

    /// <summary>Includes application-owned asynchronous work, such as an external producer, in idle detection.</summary>
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
            if (_work != 0)
            {
                throw new InvalidOperationException("Wait for harness idle before calling Reset.");
            }

            _sent.Clear();
            _dispatched.Clear();
            _consumed.Clear();
            _failed.Clear();
            _deadLettered.Clear();
            _identities.Clear();
            _handled.Clear();
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

            lock (_gate)
            {
                _identities[context.MessageId] = message;
            }

            try
            {
                await next(context).ConfigureAwait(false);

                lock (_gate)
                {
                    _sent.Add(message);
                }
            }
            catch (Exception error)
            {
                RecordFailure(message, error, HarnessOperation.Send);
                throw;
            }
        }
        finally
        {
            WorkCompleted();
        }
    }

    async ValueTask IRelayerObserver.ProcessAsync(DefaultMessageContext context, ConsumeDelegate next)
    {
        WorkStarted();

        try
        {
            var observation = Snapshot(context);

            lock (_gate)
            {
                _consumed.Add(observation);
                _failing.Remove(context);
            }

            var handled = false;

            try
            {
                await next(context).ConfigureAwait(false);
                handled = true;
            }
            finally
            {
                Processed(context, observation, handled);
            }
        }
        finally
        {
            WorkCompleted();
        }
    }

    async ValueTask IRelayerObserver.ProcessBatchAsync(ConsumePipeline owner,
        ReadOnlyMemory<DefaultMessageContext> contexts, CancellationToken cancellationToken)
    {
        WorkStarted();

        try
        {
            var observations = new HarnessMessage[contexts.Length];

            for (var i = 0; i < observations.Length; i++)
            {
                observations[i] = Snapshot(contexts.Span[i]);
            }

            lock (_gate)
            {
                for (var i = 0; i < observations.Length; i++)
                {
                    _consumed.Add(observations[i]);
                    _failing.Remove(contexts.Span[i]);
                }
            }

            var handled = false;

            try
            {
                await owner.ProcessBatchCoreAsync(contexts, cancellationToken).ConfigureAwait(false);
                handled = true;
            }
            finally
            {
                for (var i = 0; i < observations.Length; i++)
                {
                    var context = contexts.Span[i];
                    // A later item can throw after this item's native settlement completed.
                    Processed(context, observations[i], handled || context.Features.Get<SuccessfulSettlement>() is not null);
                }
            }
        }
        finally
        {
            WorkCompleted();
        }
    }

    ConsumeDelegate IRelayerObserver.WrapSettlement(ConsumeDelegate next)
    {
        return async context =>
            {
                var delivery = (DefaultMessageContext)context;
                var disposition = delivery.ObservedDisposition;
                await next(context).ConfigureAwait(false);

                if (disposition.Kind is DispositionKind.Complete or DispositionKind.SettledByHandler)
                {
                    delivery.Features.Set(SuccessfulSettlement.Instance);
                }

                // The in-memory adapter observes actual settlement, including manual native settlement.
                if (disposition.Kind == DispositionKind.DeadLetter && context.Features.Get<NativeMessage<InMemoryDelivery>>() is null)
                {
                    RecordDeadLetter(Snapshot(delivery), disposition.DeadLetter);
                }
            };
    }

    void IRelayerObserver.Failed(DefaultMessageContext context, Exception error)
    {
        lock (_gate)
        {
            _failing[context] = context.Attempt;
        }

        RecordFailure(Snapshot(context), error, HarnessOperation.Consume);
    }

    // Handled: processing returned or the batch item settled successfully, the final attempt did not fail, and
    // nothing requested or decided a retry or dead-letter. An in-process retry advances the attempt. A
    // requested retry or dead-letter is never handled, even when a copy fallback completes the original.
    private void Processed(DefaultMessageContext context, HarnessMessage observation, bool completed)
    {
        List<IConsumedWaiter>? ready = null;

        lock (_gate)
        {
            var finalAttemptFailed = _failing.Remove(context, out var attempt) && attempt == context.Attempt;

            if (!completed || finalAttemptFailed || context.Settlement.Request is { Kind: not DispositionKind.Complete } ||
                context.FailureDisposition is { Kind: not DispositionKind.Complete } ||
                context.Features.Get<NativeMessage<InMemoryDelivery>>()?.Message.SettlementKind is DispositionKind.Redeliver or DispositionKind.DeadLetter)
            {
                return;
            }

            _handled.Add(observation);

            foreach (var waiter in _waiters)
            {
                if (waiter.Enqueue(observation))
                {
                    (ready ??= []).Add(waiter);
                }
            }
        }

        if (ready is null)
        {
            return;
        }

        foreach (var waiter in ready)
        {
            Drain(waiter);
        }
    }

    private void Drain(IConsumedWaiter waiter)
    {
        while (true)
        {
            HarnessMessage? observation;

            lock (_gate)
            {
                observation = waiter.TakeNext();
            }

            if (observation is null || waiter.TryComplete(observation))
            {
                return;
            }
        }
    }

    void IRelayerObserver.DeadLettered(InMemoryDelivery delivery, DeadLetterInfo error, (string TransportName, string Source) endpoint)
    {
        var names = new HeaderNames();
        InMemoryMessageBinding.TryGetHeader(delivery, names.Id, out var id);
        InMemoryMessageBinding.TryGetHeader(delivery, names.Type, out var type);
        RecordDeadLetter(new(id.IsEmpty ? null : id.ToString(), type.IsEmpty ? null : type.ToString(),
            delivery.Message.Body.ToArray(), endpoint.TransportName, endpoint.Source, delivery.Attempt), error);
    }

    private HarnessMessage Snapshot(DefaultMessageContext context)
    {
        var id = context.GetLogMessageId();
        (string? transportName, string? destination) = context.ReceiveEndpoint is { } endpoint
            ? (endpoint.TransportName, endpoint.Source)
            : CorrelateSend(id);
        return new(id, context.GetLogMessageType(), context.Body.ToArray(), transportName, destination,
            context.Attempt, isTombstone: context.IsTombstone, tombstoneKey: context.TombstoneKey);
    }

    private (string? TransportName, string? Destination) CorrelateSend(string? id)
    {
        lock (_gate)
        {
            var original = id is not null && _identities.TryGetValue(id, out var known) ? known : null;
            return (original?.TransportName, original?.Destination);
        }
    }

    internal void RecordDispatch(HarnessMessage message)
    {
        lock (_gate)
        {
            _dispatched.Add(message);
        }
    }

    internal void Remember(HarnessMessage message)
    {
        lock (_gate)
        {
            if (message.MessageId is { } id)
            {
                _identities[id] = message;
            }
        }
    }

    internal void RecordFailure(HarnessMessage message, Exception error, HarnessOperation operation)
    {
        lock (_gate)
        {
            _failed.Add(new(message, error, operation));
        }
    }

    private void RecordDeadLetter(HarnessMessage message, DeadLetterInfo error)
    {
        var headers = WireString.CopyHeaders(error.Headers.Span);

        lock (_gate)
        {
            _deadLettered.Add(new(message, new(error.Reason, error.Description, headers)));
        }
    }

    void IRelayerObserver.OutboxCommitting(ReadOnlySpan<Guid> rows)
    {
        lock (_gate)
        {
            var wasEmpty = _outboxRows.Count == 0;

            foreach (var row in rows)
            {
                // Redrive can reinsert the same ID before its previous settlement reaches the harness.
                _outboxRows[row] = _outboxRows.GetValueOrDefault(row) + 1;
            }

            if (wasEmpty && _outboxRows.Count != 0)
            {
                WorkStarted();
            }
        }
    }

    void IRelayerObserver.OutboxSettled(Guid rowId)
    {
        lock (_gate)
        {
            if (!_outboxRows.TryGetValue(rowId, out var reservations))
            {
                return;
            }

            if (reservations > 1)
            {
                _outboxRows[rowId] = reservations - 1;
                return;
            }

            _outboxRows.Remove(rowId);

            if (_outboxRows.Count == 0)
            {
                WorkCompleted();
            }
        }
    }

    void IRelayerObserver.WorkStarted()
    {
        WorkStarted();
    }

    void IRelayerObserver.WorkCompleted()
    {
        WorkCompleted();
    }

    internal void WorkStarted()
    {
        lock (_gate)
        {
            if (_work++ == 0)
            {
                _idle = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
    }

    internal void WorkCompleted()
    {
        lock (_gate)
        {
            if (--_work == 0)
            {
                _idle.TrySetResult();
            }
        }
    }

    private static TaskCompletionSource CompletedSignal()
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        signal.SetResult();
        return signal;
    }

    private sealed class SuccessfulSettlement
    {
        internal static SuccessfulSettlement Instance
        {
            get;
        } = new();
    }

    private interface IConsumedWaiter
    {
        // Queue state is accessed under the harness gate. Predicates always run outside it.
        bool Enqueue(HarnessMessage observation);

        HarnessMessage? TakeNext();

        void ClearPending();

        bool TryComplete(HarnessMessage observation);
    }

    private sealed class ConsumedWaiter<TMessage>(RelayerHarness owner, string identity, Func<TMessage, bool>? predicate)
        : TaskCompletionSource<TMessage>(TaskCreationOptions.RunContinuationsAsynchronously), IConsumedWaiter
    {
        private readonly Queue<HarnessMessage> _pending = new();
        // Registration owns the drain while it scans history, even when the snapshot is empty.
        private bool _draining = true;

        public bool Enqueue(HarnessMessage observation)
        {
            if (Task.IsCompleted)
            {
                return false;
            }

            _pending.Enqueue(observation);

            if (_draining)
            {
                return false;
            }

            _draining = true;
            return true;
        }

        public HarnessMessage? TakeNext()
        {
            if (_pending.TryDequeue(out var observation))
            {
                return observation;
            }

            _draining = false;
            return null;
        }

        public void ClearPending()
        {
            _pending.Clear();
        }

        public bool TryComplete(HarnessMessage observation)
        {
            if (Task.IsCompleted)
            {
                return true;
            }

            if (!string.Equals(observation.WireType, identity, StringComparison.Ordinal) || !observation.Matches<TMessage>())
            {
                return false;
            }

            try
            {
                var message = owner.ReadCore<TMessage>(observation);

                if (predicate is not null && !predicate(message))
                {
                    return false;
                }

                TrySetResult(message);
                return true;
            }
            catch (Exception error)
            {
                // Surface a deserialization or predicate failure to the waiting test, not the consumer.
                TrySetException(error);
                return true;
            }
        }
    }

    private sealed class WorkLease(RelayerHarness owner) : IDisposable
    {
        private RelayerHarness? _owner = owner;

        public void Dispose()
        {
            Interlocked.Exchange(ref _owner, null)?.WorkCompleted();
        }
    }
}
