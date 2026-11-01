using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks.Sources;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer;

internal sealed class SendRoute<TMessage>
{
    private readonly ITransport _transport;
    private readonly string _destination;
    private readonly MessageType _messageType;
    private readonly SendRouteOptions _options;
    private readonly IOrderingKeySelector<TMessage>? _orderingKey;
    private readonly IMessageSerializer _serializer;
    private readonly SendDelegate _pipeline;
    private readonly TransportCapabilities _capabilities;
    private readonly MessageEnvelope _envelope;
    private readonly SendTracing? _tracing;
    private readonly string _wireType;
    private readonly PoolSegment _operations = new();

    internal SendRoute(ITransport transport, string destination, MessageType messageType, SendRouteOptions options,
        IOrderingKeySelector<TMessage>? orderingKey, IMessageSerializer serializer, SendDelegate pipeline,
        TransportCapabilities capabilities)
    {
        _transport = transport;
        _destination = destination;
        _messageType = messageType;
        _options = options;
        _orderingKey = orderingKey;
        _serializer = serializer;
        _pipeline = pipeline;
        _capabilities = capabilities;
        _envelope = new(messageType, options.Source, options.HeaderNames, options.EnvelopeMode);
        _tracing = (transport as ITransportSendTracing)?.GetSendTracing(destination);
        _wireType = messageType.ToString();
    }

    internal ValueTask SendAsync(TMessage message, Action<SendOptions>? configure,
        IOutboxEnlistment? outbox, CancellationToken callerToken, AttemptSendBuffer? buffer) => Rent().Start(message, configure, outbox, callerToken, buffer);

    private Operation Rent()
    {
        for (var segment = _operations; segment is not null; segment = Volatile.Read(ref segment.Next))
        {
            for (var i = 0; i < segment.Items.Length; i++)
            {
                if (Interlocked.Exchange(ref segment.Items[i], null) is { } operation)
                {
                    return operation;
                }
            }
        }
        return new Operation(this);
    }

    private void Return(Operation operation)
    {
        var segment = _operations;
        while (true)
        {
            for (var i = 0; i < segment.Items.Length; i++)
            {
                if (Interlocked.CompareExchange(ref segment.Items[i], operation, null) is null)
                {
                    return;
                }
            }
            // Retain the peak number of overlapping sends instead of allocating again on every burst.
            var next = Volatile.Read(ref segment.Next);
            if (next is null)
            {
                var added = new PoolSegment();
                next = Interlocked.CompareExchange(ref segment.Next, added, null) ?? added;
            }
            segment = next;
        }
    }

    private sealed class PoolSegment
    {
        internal readonly Operation?[] Items = new Operation?[Math.Max(1, Environment.ProcessorCount)];
        internal PoolSegment? Next;
    }

    private sealed class Operation : SendContext, ISendOperation, IValueTaskSource
    {
        private readonly SendRoute<TMessage> _route;
        private readonly Action _continuation;
        private ManualResetValueTaskSourceCore<bool> _completion;
        private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _pending;
        private CancellationToken _callerToken;
        private CancellationTokenRegistration _registration;
        private TMessage _message = default!;
        private IOutboxEnlistment? _outbox;
        private AttemptSendBuffer? _buffer;
        private MessageEnvelopeMetadata _metadata;
        private string? _messageId;
        private Activity? _activity;
        private bool _tracingStarted;
        private readonly ArrayBufferWriter<byte> _body = new(256);
        private readonly ArrayBufferWriter<byte> _key = new(64);
        private readonly SendHeaders _headers = new();

        internal Operation(SendRoute<TMessage> route)
        {
            _route = route;
            _continuation = Complete;
        }

        internal CancellationTokenSource Timeout { get; private set; } = new();
        public override object Message => _message!;
        public string TransportName => _route._transport.Name;
        public string Destination => _route._destination;
        public bool UsesOutbox => _outbox is not null;
        public override MessageType MessageType => _route._messageType;
        public override string MessageId => _messageId ??= _metadata.MessageId.ToString("D");
        public override SendOptions Options { get; } = new();
        public override CancellationToken CancellationToken => Timeout.Token;

        internal ValueTask Start(TMessage message, Action<SendOptions>? configure, IOutboxEnlistment? outbox, CancellationToken callerToken, AttemptSendBuffer? buffer)
        {
            _message = message;
            _outbox = outbox;
            _buffer = buffer;
            _callerToken = callerToken;
            _metadata = MessageEnvelopeMetadata.Create(Activity.Current?.Context ?? default);
            var previousActivity = Activity.Current;
            Exception? error = null;
            try
            {
                Timeout.CancelAfter(_route._options.Timeout);
                _registration = callerToken.UnsafeRegister(static state => ((Operation)state!).Timeout.Cancel(), this);
                configure?.Invoke(Options);
                CancellationToken.ThrowIfCancellationRequested();
                _pending = _route._pipeline(this).ConfigureAwait(false).GetAwaiter();
                if (!_pending.IsCompleted)
                {
                    _completion.Reset();
                    var version = _completion.Version;
                    _pending.UnsafeOnCompleted(_continuation);
                    return new ValueTask(this, version);
                }
                _pending.GetResult();
            }
            catch (Exception exception)
            {
                error = Translate(exception);
                _route._tracing?.Fail(_activity, error);
            }
            finally { Activity.Current = previousActivity; }
            Reset();
            _route.Return(this);
            if (error is OperationCanceledException)
            {
                return ValueTask.FromCanceled(callerToken.IsCancellationRequested ? callerToken : new CancellationToken(true));
            }
            return error is null ? ValueTask.CompletedTask : ValueTask.FromException(error);
        }

        private void Complete()
        {
            Exception? error = null;
            try { _pending.GetResult(); }
            catch (Exception exception)
            {
                error = Translate(exception);
                _route._tracing?.Fail(_activity, error);
            }
            Reset();
            // Cleanup precedes completion: an inline caller continuation may immediately rent this operation again.
            if (error is null) _completion.SetResult(true);
            else _completion.SetException(error);
        }

        private Exception Translate(Exception exception) =>
            exception is OperationCanceledException && !_callerToken.IsCancellationRequested && Timeout.IsCancellationRequested
                ? new TimeoutException("The send exceeded its route timeout. Check broker connectivity or increase SendRouteOptions.Timeout.", exception)
                : exception;

        public void GetResult(short token)
        {
            // An operation remains reserved until the caller consumes its ValueTask, even if transport completion was earlier.
            try { _completion.GetResult(token); }
            finally { _route.Return(this); }
        }

        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _completion.OnCompleted(continuation, state, token, flags);

        public ValueTask DispatchAsync()
        {
            CancellationToken.ThrowIfCancellationRequested();
            if (!_tracingStarted && _outbox is null && _route._tracing is { } tracing)
            {
                var external = default(ActivityContext);
                if (_route._options.EnvelopeMode == MessageEnvelopeMode.Raw &&
                    Options.Headers.TryGetValue(_route._options.HeaderNames.TraceParent, out var parent))
                {
                    Options.Headers.TryGetValue(_route._options.HeaderNames.TraceState, out var state);
                    ActivityContext.TryParse(parent, state, true, out external);
                }
                _activity = tracing.Start(external, _buffer is not null);
                _metadata = _metadata with { CreationContext = external != default ? external : _activity?.Context ?? _metadata.CreationContext };
                _tracingStarted = true;
            }
            if (Options.OrderingKey is not null && !_route._capabilities.OrderedByKey)
            {
                throw new InvalidOperationException($"Send route '{_route._messageType}' on transport '{_route._transport.Name}' to endpoint '{_route._destination}' specifies an OrderingKey, but the endpoint does not support OrderedByKey. Configure an endpoint with native ordering or remove the OrderingKey override.");
            }
            if (_outbox is null && Options.DeliverAt is { } deliverAt &&
                (_route._capabilities.MaxDeliveryDelay is not { } maxDelay || deliverAt - DateTimeOffset.UtcNow > maxDelay))
            {
                throw new InvalidOperationException($"Send route '{_route._messageType}' on transport '{_route._transport.Name}' to endpoint '{_route._destination}' specifies a DeliverAt outside the endpoint's MaxDeliveryDelay. Use an endpoint with a sufficient native delay, shorten or remove DeliverAt, or send through an active outbox transaction.");
            }
            if (_outbox is not null && Options.HasNativeConfigurators)
            {
                // Reject before serialization or any durable side effect.
                new OutgoingMessage(_route._destination, default, options: Options).ValidateForOutbox();
            }
            // Middleware may retry next after a completed failure. Keep the identity, rebuild the payload.
            _body.Clear();
            _headers.Clear();
            _key.Clear();
            _route._serializer.Serialize(_message, _body);
            _route._envelope.Write<SendHeaders, SendHeaders>(_headers, _metadata);
            _headers.Add("content-type", new WireString(_route._serializer.ContentType));
            foreach (var header in Options.Headers)
            {
                if (_route._options.EnvelopeMode == MessageEnvelopeMode.CloudEvents && IsEnvelopeHeader(header.Key))
                {
                    throw new InvalidOperationException("SendOptions.Headers cannot override CloudEvents or creation-context headers. Use route envelope settings or raw mode.");
                }
                _headers.Add(header.Key, new WireString(header.Value));
            }

            var outgoing = new OutgoingMessage(_route._destination, _body.WrittenMemory, _headers.Written,
                Options.OrderingKey is { } key ? new WireString(key) : _route._orderingKey?.Select(_message, _key), Options.DeliverAt, Options);
            if (_activity is { IsAllDataRequested: true } activity)
            {
                activity.SetTag("messaging.message.id", MessageId);
                activity.SetTag("cloudevents.event_type", _route._wireType);
                activity.SetTag("cloudevents.event_source", _route._options.Source);
                if (_route._tracing!.IncludeBodySize) activity.SetTag("messaging.message.body.size", _body.WrittenCount);
                if (Options.Headers.TryGetValue(_route._options.HeaderNames.CorrelationId, out var correlation))
                    activity.SetTag("messaging.message.conversation_id", correlation);
            }
            if (_buffer is null && _outbox is null && _route._tracing is not null) outgoing = outgoing.WithTracingStarted();
            CancellationToken.ThrowIfCancellationRequested();
            if (_outbox is null && _buffer is not null)
            {
                _buffer.Add(_route._transport, outgoing, _route._options.Timeout);
                return ValueTask.CompletedTask;
            }
            return _outbox is null
                ? _route._transport.SendAsync(outgoing, CancellationToken)
                : _outbox.EnlistAsync(new OutboxRecord(_metadata.MessageId, _route._transport.Name,
                    _route._wireType, outgoing, _metadata.Time), CancellationToken);
        }

        private bool IsEnvelopeHeader(string name)
        {
            var names = _route._options.HeaderNames;
            return name == names.Id || name == names.Type || name == names.Source || name == names.Time ||
                name == names.SpecVersion || name == names.TraceParent || name == names.TraceState;
        }

        internal void Reset()
        {
            var current = Activity.Current;
            _activity?.Dispose();
            if (!ReferenceEquals(current, _activity)) Activity.Current = current;
            _activity = null;
            _tracingStarted = false;
            // Dispose waits for a caller cancellation callback before its source can be reused.
            _registration.Dispose();
            _registration = default;
            Timeout.CancelAfter(System.Threading.Timeout.InfiniteTimeSpan);
            if (!Timeout.TryReset())
            {
                Timeout.Dispose();
                Timeout = new();
            }
            _message = default!;
            _outbox = null;
            _buffer = null;
            _metadata = default;
            _messageId = null;
            _pending = default;
            _callerToken = default;
            Options.Reset();
            _body.Clear();
            _key.Clear();
            _headers.Clear();
        }
    }
}

/// <summary>Reusable storage for the transport-neutral envelope. Values remain valid through send completion.</summary>
/// <remarks>Header value buffers grow on demand and are retained per pooled operation for subsequent sends.</remarks>
internal sealed class SendHeaders : IMessageHeaderWriter<SendHeaders>, IMessageHeaderReader<SendHeaders>
{
    private KeyValuePair<string, WireString>[] _entries = new KeyValuePair<string, WireString>[8];
    private byte[]?[] _values = new byte[8][];
    private int _count;

    internal ReadOnlyMemory<KeyValuePair<string, WireString>> Written => _entries.AsMemory(0, _count);

    public static bool TryGetHeader(SendHeaders message, string name, out WireString value)
    {
        var index = message.Find(name);
        value = index >= 0 ? message._entries[index].Value : default;
        return index >= 0;
    }

    internal void Add(string name, WireString value)
    {
        var index = Find(name);
        if (index < 0)
        {
            index = Append();
        }
        _entries[index] = new(name, value);
    }

    public static void SetHeader(SendHeaders message, string name, ReadOnlySpan<char> value)
    {
        var index = message.Find(name);
        if (index < 0)
        {
            index = message.Append();
        }
        var length = Encoding.UTF8.GetByteCount(value);
        var buffer = message._values[index];
        if (buffer is null || buffer.Length < length)
        {
            buffer = new byte[Math.Max(128, length)];
            message._values[index] = buffer;
        }
        Encoding.UTF8.GetBytes(value, buffer);
        message._entries[index] = new(name, new WireString(buffer.AsMemory(0, length)));
    }

    public static void RemoveHeader(SendHeaders message, string name)
    {
        var index = message.Find(name);
        if (index >= 0)
        {
            // Keep value storage at its index: existing header slices must not be overwritten.
            Array.Copy(message._entries, index + 1, message._entries, index, message._count - index - 1);
            Array.Copy(message._values, index + 1, message._values, index, message._count - index - 1);
            message._entries[--message._count] = default;
            message._values[message._count] = null;
        }
    }

    private int Find(string name)
    {
        for (var i = 0; i < _count; i++)
        {
            if (_entries[i].Key == name)
            {
                return i;
            }
        }
        return -1;
    }

    private int Append()
    {
        if (_count == _entries.Length)
        {
            Array.Resize(ref _entries, _count * 2);
            Array.Resize(ref _values, _count * 2);
        }
        return _count++;
    }

    internal void Clear()
    {
        Array.Clear(_entries, 0, _count);
        _count = 0;
    }
}
