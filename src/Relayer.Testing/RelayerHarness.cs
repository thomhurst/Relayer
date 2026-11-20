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
    private long _outboxRows;

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

    /// <summary>Failed sends, dispatches and consume attempts.</summary>
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

    /// <summary>Waits for a delivery of the registered message identity that the consume pipeline handled successfully, without polling.</summary>
    /// <remarks>Includes deliveries handled before the call. Failed attempts and dead-lettered deliveries do not match; a later successful redelivery does.
    /// Use this for broker round-trips, where idle cannot be inferred. The predicate runs on the consuming thread and must not block.</remarks>
    public async Task<TMessage> WaitForConsumedAsync<TMessage>(Func<TMessage, bool>? predicate = null, CancellationToken cancellationToken = default)
    {
        var waiter = new ConsumedWaiter<TMessage>(this, GetIdentity<TMessage>(), predicate);
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

            return await waiter.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            lock (_gate)
            {
                _waiters.Remove(waiter);
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
                    Processed(contexts.Span[i], observations[i], handled);
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

    // Handled: processing returned, the final attempt did not fail, and nothing requested or decided a retry or
    // dead-letter. An in-process retry advances the attempt, so an earlier failed attempt does not count. A
    // requested retry or dead-letter is never handled, even when a copy fallback completes the original.
    private void Processed(DefaultMessageContext context, HarnessMessage observation, bool returned)
    {
        IConsumedWaiter[] waiters;

        lock (_gate)
        {
            var finalAttemptFailed = _failing.Remove(context, out var attempt) && attempt == context.Attempt;

            if (!returned || finalAttemptFailed || context.Settlement.Request is { Kind: not DispositionKind.Complete } ||
                context.FailureDisposition is { Kind: not DispositionKind.Complete })
            {
                return;
            }

            _handled.Add(observation);

            if (_waiters.Count == 0)
            {
                return;
            }

            waiters = _waiters.ToArray();
        }

        foreach (var waiter in waiters)
        {
            waiter.TryComplete(observation);
        }
    }

    void IRelayerObserver.DeadLettered(InMemoryDelivery delivery, DeadLetterInfo error)
    {
        var names = new HeaderNames();
        InMemoryMessageBinding.TryGetHeader(delivery, names.Id, out var id);
        InMemoryMessageBinding.TryGetHeader(delivery, names.Type, out var type);
        RecordDeadLetter(Correlate(id.IsEmpty ? null : id.ToString(), type.IsEmpty ? null : type.ToString(),
            delivery.Message.Body.ToArray(), delivery.Attempt), error);
    }

    private HarnessMessage Snapshot(DefaultMessageContext context)
    {
        return Correlate(context.GetLogMessageId(),
                context.GetLogMessageType(), context.Body.ToArray(), context.Attempt,
                isTombstone: context.IsTombstone, tombstoneKey: context.TombstoneKey);
    }

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
        var headers = new KeyValuePair<string, WireString>[error.Headers.Length];

        for (var i = 0; i < headers.Length; i++)
        {
            headers[i] = new(error.Headers.Span[i].Key, new WireString(error.Headers.Span[i].Value.ToString()));
        }

        lock (_gate)
        {
            _deadLettered.Add(new(message, new(error.Reason, error.Description, headers)));
        }
    }

    // A dispatcher can settle a row before its producer reports the commit, so the count may briefly go negative.
    void IRelayerObserver.OutboxPending(long delta)
    {
        lock (_gate)
        {
            var before = _outboxRows;
            _outboxRows += delta;

            if (before <= 0 && _outboxRows > 0)
            {
                WorkStarted();
            }
            else if (before > 0 && _outboxRows <= 0)
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

    private interface IConsumedWaiter
    {
        bool TryComplete(HarnessMessage observation);
    }

    private sealed class ConsumedWaiter<TMessage>(RelayerHarness owner, string identity, Func<TMessage, bool>? predicate)
        : TaskCompletionSource<TMessage>(TaskCreationOptions.RunContinuationsAsynchronously), IConsumedWaiter
    {
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
