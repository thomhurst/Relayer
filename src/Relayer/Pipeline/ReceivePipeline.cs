using Relayer.Transports;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace Relayer.Pipeline;

/// <summary>Feeds untouched native deliveries into a composed consumer and returns native settlement decisions.</summary>
/// <remarks>
/// Compose once per consumer. Body and header storage must remain valid until processing completes.
/// This adapter invokes the consumer's optional native settlement callback; it does not implement capability fallbacks. The configured failure delegate
/// sets NativeMessage&lt;TIncoming&gt;.Disposition after durable failure handling. Exceptions and cancellation
/// propagate without returning a settlement decision. Size both pools for maximum concurrent deliveries.
/// </remarks>
public sealed class ReceivePipeline<TIncoming, TBinding>
    where TBinding : struct, IMessageBinding<TIncoming>
{
    private readonly ConsumePipeline _consumer;
    private readonly HeaderNames _names;
    private readonly SlotPool<State> _states;
    private readonly SlotPool<BatchState>? _batches;
    private int _batchCursor;
    private readonly bool _preserveSettledFailures;

    /// <summary>Adapts a consumer with its transport's CloudEvents header binding.</summary>
    public ReceivePipeline(ConsumePipeline consumer, HeaderNames names, int contextPoolCapacity = 64)
        : this(consumer, names, contextPoolCapacity, false) { }

    internal ReceivePipeline(ConsumePipeline consumer, HeaderNames names, int contextPoolCapacity, bool preserveSettledFailures)
    {
        _preserveSettledFailures = preserveSettledFailures;
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentNullException.ThrowIfNull(names);
        ArgumentOutOfRangeException.ThrowIfLessThan(contextPoolCapacity, 1);
        _consumer = consumer;
        _names = names;
        _states = new(contextPoolCapacity, contextPoolCapacity);
        for (var i = 0; i < contextPoolCapacity; i++) _states.TryReturn(new State(consumer, names, preserveSettledFailures, i), i);
        if (consumer.BatchExecution is { } batch)
        {
            var capacity = Math.Max(1, contextPoolCapacity / batch.Size);
            _batches = new(capacity, capacity);
            for (var i = 0; i < capacity; i++) _batches.TryReturn(new BatchState(batch.Size), i);
        }
    }

    /// <summary>Processes one delivery and releases all native references before returning its disposition.</summary>
    public ValueTask<Disposition> ProcessAsync(TIncoming message, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested) return ValueTask.FromCanceled<Disposition>(cancellationToken);
        return Rent().Start(this, message, cancellationToken);
    }

    /// <summary>Processes one delivery settled by the consumer's native settlement callback.</summary>
    /// <remarks>
    /// Use this overload for a native callback that needs no settlement decision: it avoids carrying a
    /// <see cref="Disposition"/> through the callback's returned task.
    /// </remarks>
    public ValueTask ProcessSettledAsync(TIncoming message, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested) return ValueTask.FromCanceled(cancellationToken);
        return Rent().StartSettled(this, message, cancellationToken);
    }

    /// <summary>Processes deliveries in input order, writing one disposition per delivery.</summary>
    /// <remarks>
    /// A consumer built with BuildBatch dispatches its generated batch handler; other consumers process deliveries individually. Output may be larger than input;
    /// its unused tail is untouched. Only settle from this output when the entire call succeeds.
    /// On failure or cancellation, some earlier deliveries may have completed durable work; native redelivery
    /// remains at-least-once. Both memories must remain valid and unchanged until the call completes.
    /// </remarks>
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask ProcessBatchAsync(ReadOnlyMemory<TIncoming> messages, Memory<Disposition> dispositions,
        CancellationToken cancellationToken = default)
    {
        if (dispositions.Length < messages.Length)
            throw new ArgumentException("Provide at least one disposition slot per incoming message.", nameof(dispositions));
        cancellationToken.ThrowIfCancellationRequested();
        if (_consumer.IsBatch)
        {
            await ProcessHandlerBatchAsync(messages, dispositions, cancellationToken).ConfigureAwait(false);
            return;
        }
        for (var i = 0; i < messages.Length; i++)
        {
            var disposition = await ProcessAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
            dispositions.Span[i] = disposition;
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessHandlerBatchAsync(ReadOnlyMemory<TIncoming> messages, Memory<Disposition> dispositions,
        CancellationToken cancellationToken)
    {
        var maximum = _consumer.BatchExecution!.Size;
        if (messages.Length > maximum) throw new ArgumentOutOfRangeException(nameof(messages), "Batch exceeds the configured maximum size.");
        if (messages.IsEmpty) return;
        var batch = _batches!.TryRent() ?? new BatchState(maximum);
        var count = 0;
        try
        {
            for (; count < messages.Length; count++)
            {
                var state = Rent();
                batch.States[count] = state;
                state.Initialize(messages.Span[count]);
                var context = _consumer.RentContext(state, cancellationToken);
                batch.Contexts[count] = context;
                context.Features.Set(state.Native);
            }
            await _consumer.ProcessBatchAsync(batch.Contexts.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < count; i++)
            {
                var native = batch.States[i]!.Native.Disposition;
                dispositions.Span[i] = native.Kind == DispositionKind.SettledByHandler
                    ? native : batch.Contexts[i].FailureDisposition ?? native;
            }
        }
        finally
        {
            // Include the slot whose native binding or context initialization threw.
            for (var i = 0; i < messages.Length; i++)
            {
                batch.Contexts[i]?.Dispose();
                batch.Contexts[i] = null!;
                if (batch.States[i] is not { } state) continue;
                state.Clear();
                Return(state);
                batch.States[i] = null;
            }
            _batches.TryReturn(batch);
        }
    }

    private sealed class BatchState(int size)
    {
        internal readonly DefaultMessageContext[] Contexts = new DefaultMessageContext[size];
        internal readonly State?[] States = new State[size];
    }

    private State Rent()
        => (_consumer.IsBatch ? _states.TryRent(ref _batchCursor) : _states.TryRent())
            ?? new State(_consumer, _names, _preserveSettledFailures);

    private void Return(State state)
    {
        if (_consumer.IsBatch) _states.TryReturn(state, state.PoolSlot);
        else _states.TryReturn(state);
    }

    private sealed class State : ReceiveMessageState, IValueTaskSource<Disposition>, IValueTaskSource
    {
        private readonly ConsumePipeline consumer;
        private readonly HeaderNames names;
        private readonly Action _continuation;
        private ReceivePipeline<TIncoming, TBinding> _owner = null!;
        private DefaultMessageContext? _context;
        private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _pending;
        private ManualResetValueTaskSourceCore<Disposition> _completion;
        private ExecutionContext? _executionContext;
        private Disposition _result;
        private Exception? _error;
        private bool _asynchronous;
        private readonly bool _preserveSettledFailures;

        internal State(ConsumePipeline consumer, HeaderNames names, bool preserveSettledFailures, int poolSlot = -1)
        {
            _preserveSettledFailures = preserveSettledFailures;
            this.consumer = consumer;
            this.names = names;
            PoolSlot = poolSlot;
            _continuation = Complete;
        }

        internal ValueTask<Disposition> Start(ReceivePipeline<TIncoming, TBinding> owner, TIncoming message, CancellationToken cancellationToken)
        {
            Run(owner, message, cancellationToken);
            if (_asynchronous) return new ValueTask<Disposition>(this, _completion.Version);
            var result = _result;
            var failure = Release(owner);
            return failure is null ? new ValueTask<Disposition>(result) : ValueTask.FromException<Disposition>(failure);
        }

        internal ValueTask StartSettled(ReceivePipeline<TIncoming, TBinding> owner, TIncoming message, CancellationToken cancellationToken)
        {
            Run(owner, message, cancellationToken);
            if (_asynchronous) return new ValueTask(this, _completion.Version);
            var failure = Release(owner);
            return failure is null ? default : ValueTask.FromException(failure);
        }

        private void Run(ReceivePipeline<TIncoming, TBinding> owner, TIncoming message, CancellationToken cancellationToken)
        {
            _owner = owner;
            var runner = new Runner(this, message, cancellationToken);
            var builder = AsyncTaskMethodBuilder.Create();
            builder.Start(ref runner);
        }

        private Exception? Release(ReceivePipeline<TIncoming, TBinding> owner)
        {
            var failure = _error;
            _result = default;
            _error = null;
            _owner = null!;
            owner.Return(this);
            return failure;
        }

        private void Begin(TIncoming message, CancellationToken cancellationToken)
        {
            _asynchronous = false;
            try
            {
                Initialize(message);
                _context = consumer.RentContext(this, cancellationToken);
                _context.Features.Set(Native);
                _pending = consumer.ProcessAsync(_context).ConfigureAwait(false).GetAwaiter();
                if (!_pending.IsCompleted)
                {
                    _completion.Reset();
                    _asynchronous = true;
                    _executionContext = ExecutionContext.Capture();
                    _pending.UnsafeOnCompleted(_continuation);
                    return;
                }
                _pending.GetResult();
                _result = Native.Disposition;
            }
            catch (Exception error) { _error = error; }
            Cleanup();
        }

        private struct Runner(State state, TIncoming message, CancellationToken cancellationToken) : IAsyncStateMachine
        {
            public void MoveNext() => state.Begin(message, cancellationToken);
            public void SetStateMachine(IAsyncStateMachine stateMachine) => throw new NotSupportedException();
        }
        private void Complete()
        {
            var executionContext = _executionContext;
            _executionContext = null;
            if (executionContext is not null)
                ExecutionContext.Run(executionContext, static state => ((State)state!).CompleteCore(), this);
            else CompleteCore();
        }

        private void CompleteCore()
        {
            try
            {
                _pending.GetResult();
                _result = Native.Disposition;
            }
            catch (Exception error) { _error = error; }
            Cleanup();
            var errorResult = _error;
            _error = null;
            if (errorResult is null) _completion.SetResult(_result);
            else _completion.SetException(errorResult);
        }

        private void Cleanup()
        {
            try { _context?.Dispose(); }
            catch (Exception error) { _error = error; }
            finally
            {
                if (_error is not null && _preserveSettledFailures && Native.Disposition.Kind == DispositionKind.SettledByHandler)
                    _error = new SettledReceiveException(_error);
                _context = null;
                _pending = default;
                Clear();
            }
        }

        public Disposition GetResult(short token)
        {
            // Keep continuation storage reserved until its ValueTask is consumed.
            try { return _completion.GetResult(token); }
            finally
            {
                _completion.Reset();
                _result = default;
                var owner = _owner;
                _owner = null!;
                owner.Return(this);
            }
        }
        void IValueTaskSource.GetResult(short token) => GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            => _completion.OnCompleted(continuation, state, token, flags);

        internal override HeaderNames EnvelopeNames => names;
        internal int PoolSlot { get; }
        private MessageHeaders<TIncoming, TBinding> _headers;
        private WireString _id;
        private string? _messageId;
        private MessageType? _messageType;
        private ConsumeDelegate? _dispatch;
        private int _attempt;
        private ReadOnlySequence<byte> _body;
        internal NativeMessage<TIncoming> Native { get; } = new();

        internal override void SetDisposition(Disposition disposition) => Native.Disposition = disposition;
        internal override Disposition Disposition => Native.Disposition;

        internal void Initialize(TIncoming message)
        {
            Native.Message = message;
            _headers = new(message, names);
            _body = TBinding.GetBody(message);
            _attempt = TBinding.GetDeliveryCount(message) ?? 1;
            ArgumentOutOfRangeException.ThrowIfLessThan(_attempt, 1);
        }

        internal override string MessageId
        {
            get
            {
                if (_messageId is not null) return _messageId;
                if (_id.IsEmpty && (!TryGetValue(names.Id, out _id) || _id.IsEmpty))
                    throw new MessageEnvelopeException($"The incoming message has no '{names.Id}' header.");
                return _messageId = _id.ToString();
            }
        }

        internal override MessageType MessageType
        {
            get
            {
                if (_messageType is { } identity) return identity;
                if (!_headers.TryGetValue(names.Type, out var type) || type.IsEmpty)
                    throw new MessageEnvelopeException($"The incoming message has no '{names.Type}' header.");
                var resolved = consumer.ResolveWireIdentity(type, out _dispatch);
                _messageType = resolved;
                if (!TryGetValue(names.Id, out _id) || _id.IsEmpty)
                    throw new MessageEnvelopeException($"The incoming message has no '{names.Id}' header.");
                return resolved;
            }
        }
        internal override ConsumeDelegate? ResolvedDispatch
        {
            get
            {
                _ = MessageType;
                return _dispatch;
            }
        }
        internal override int Attempt => _attempt;
        internal override ReadOnlySequence<byte> Body => _body;
        internal override bool IsTombstone => TBinding.IsTombstone(Native.Message);
        internal override WireString? TombstoneKey => TBinding.GetTombstoneKey(Native.Message);
        internal override MessageOperationTracing? ProcessTracing => TBinding.GetProcessTracing(Native.Message);
        internal override string? TracePartitionId => TBinding.GetPartitionId(Native.Message);
        internal override void EnrichActivity(System.Diagnostics.Activity activity) => TBinding.EnrichActivity(activity, Native.Message);
        internal override bool TryConsumeLoggedSettlementError(Exception error) => TBinding.TryConsumeLoggedSettlementError(Native.Message, error);
        public override bool TryGetValue(string name, out WireString value) => _headers.TryGetValue(name, out value);

        internal void Clear()
        {
            Native.Clear();
            _headers = default;
            _id = default;
            _messageId = null;
            _messageType = null;
            _dispatch = null;
            _attempt = 0;
            _body = default;
        }
    }
}

// Opt-in transport signal: the native disposition would otherwise be cleared before the failure escapes.
internal sealed class SettledReceiveException(Exception error) : Exception("Processing failed after native settlement.", error);
