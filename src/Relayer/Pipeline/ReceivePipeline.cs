using System.Buffers;
using System.Runtime.CompilerServices;

namespace Relayer;

/// <summary>Feeds untouched native deliveries into a composed consumer and returns native settlement decisions.</summary>
/// <remarks>
/// Compose once per consumer. Body and header storage must remain valid until processing completes.
/// This adapter does not settle messages or implement capability fallbacks. The configured failure delegate
/// sets NativeMessage&lt;TIncoming&gt;.Disposition after durable failure handling. Exceptions and cancellation
/// propagate without returning a settlement decision. Size both pools for maximum concurrent deliveries.
/// </remarks>
public sealed class ReceivePipeline<TIncoming, TBinding>
    where TBinding : struct, IMessageBinding<TIncoming>
{
    private readonly ConsumePipeline _consumer;
    private readonly HeaderNames _names;
    private readonly State?[] _states;

    /// <summary>Adapts a consumer with its transport's CloudEvents header binding.</summary>
    public ReceivePipeline(ConsumePipeline consumer, HeaderNames names, int contextPoolCapacity = 64)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentNullException.ThrowIfNull(names);
        ArgumentOutOfRangeException.ThrowIfLessThan(contextPoolCapacity, 1);
        _consumer = consumer;
        _names = names;
        _states = new State[contextPoolCapacity];
        for (var i = 0; i < _states.Length; i++) _states[i] = new State(consumer, names);
    }

    /// <summary>Processes one delivery and releases all native references before returning its disposition.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<Disposition> ProcessAsync(TIncoming message, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var state = Rent();
        try
        {
            state.Initialize(message);
            using var context = _consumer.RentContext(state, cancellationToken);
            context.Features.Set(state.Native);
            await _consumer.ProcessAsync(context).ConfigureAwait(false);
            return state.Native.Disposition;
        }
        finally
        {
            state.Clear();
            Return(state);
        }
    }

    /// <summary>Processes deliveries in input order, writing one disposition per delivery.</summary>
    /// <remarks>
    /// This is a transport batch entry point, not batch-handler dispatch. Output may be larger than input;
    /// its unused tail is untouched. Only settle from this output when the entire call succeeds.
    /// On failure or cancellation, some earlier deliveries may have completed durable work; native redelivery
    /// remains at-least-once. Both memories must remain valid and unchanged until the call completes.
    /// </remarks>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask ProcessBatchAsync(ReadOnlyMemory<TIncoming> messages, Memory<Disposition> dispositions,
        CancellationToken cancellationToken = default)
    {
        if (dispositions.Length < messages.Length)
            throw new ArgumentException("Provide at least one disposition slot per incoming message.", nameof(dispositions));
        cancellationToken.ThrowIfCancellationRequested();
        for (var i = 0; i < messages.Length; i++)
        {
            var disposition = await ProcessAsync(messages.Span[i], cancellationToken).ConfigureAwait(false);
            dispositions.Span[i] = disposition;
        }
    }

    private State Rent()
    {
        for (var i = 0; i < _states.Length; i++)
            if (Interlocked.Exchange(ref _states[i], null) is { } state) return state;
        return new State(_consumer, _names);
    }

    private void Return(State state)
    {
        for (var i = 0; i < _states.Length; i++)
            if (Interlocked.CompareExchange(ref _states[i], state, null) is null) return;
    }

    private sealed class State(ConsumePipeline consumer, HeaderNames names) : ReceiveMessageState
    {
        private MessageHeaders<TIncoming, TBinding> _headers;
        private WireString _id;
        private string? _messageId;
        private MessageType? _messageType;
        private int _attempt;
        private ReadOnlySequence<byte> _body;
        internal NativeMessage<TIncoming> Native { get; } = new();

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
                var resolved = consumer.ResolveWireIdentity(type);
                _messageType = resolved;
                if (!TryGetValue(names.Id, out _id) || _id.IsEmpty)
                    throw new MessageEnvelopeException($"The incoming message has no '{names.Id}' header.");
                return resolved;
            }
        }
        internal override int Attempt => _attempt;
        internal override ReadOnlySequence<byte> Body => _body;
        public override bool TryGetValue(string name, out WireString value) => _headers.TryGetValue(name, out value);

        internal void Clear()
        {
            Native.Clear();
            _headers = default;
            _id = default;
            _messageId = null;
            _messageType = null;
            _attempt = 0;
            _body = default;
        }
    }
}
