using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Relayer;

/// <summary>Exposes an adapter's precomputed send tracing to the serialization pipeline.</summary>
public interface ITransportSendTracing
{
    /// <summary>Returns the tracing operation for a declared native destination, or null when tracing is not configured.</summary>
    SendTracing? GetSendTracing(string destination);
}

/// <summary>Composes single and batch tracing around native send delegates without changing native delivery semantics.</summary>
/// <remarks>Construct once per endpoint. Delegates must observe the borrowed-memory contract of ITransport.</remarks>
public sealed class SendTracing
{
    private readonly MessageTracing _tracing;
    private readonly MessageOperationTracing _send;
    private readonly MessageOperationTracing _create;
    private readonly Func<OutgoingMessage, CancellationToken, ValueTask> _sendNative;
    private readonly Func<ReadOnlyMemory<OutgoingMessage>, CancellationToken, ValueTask> _sendBatchNative;
    private readonly ILogger _logger;
    private readonly HeaderNames _names;
    private readonly EndpointTelemetry? _telemetry;
    private readonly PoolSegment _states = new();

    /// <summary>Precomputes endpoint names and attributes supplied by the native adapter.</summary>
    public SendTracing(MessageTracing tracing, MessagingEndpoint endpoint,
        Func<OutgoingMessage, CancellationToken, ValueTask> send,
        Func<ReadOnlyMemory<OutgoingMessage>, CancellationToken, ValueTask> sendBatch,
        ILogger? logger = null, HeaderNames? headerNames = null)
    {
        ArgumentNullException.ThrowIfNull(tracing);
        ArgumentNullException.ThrowIfNull(send);
        ArgumentNullException.ThrowIfNull(sendBatch);
        _tracing = tracing;
        _send = new(tracing, endpoint, MessagingOperation.Send);
        _create = new(tracing, endpoint, MessagingOperation.Create);
        _sendNative = send;
        _sendBatchNative = sendBatch;
        _logger = logger ?? NullLogger.Instance;
        _names = headerNames ?? new();
        _telemetry = tracing.Instrumentation?.CreateEndpoint(new()
        {
            System = endpoint.System, Destination = endpoint.Destination, OperationName = "send",
            ServerAddress = endpoint.ServerAddress, ServerPort = endpoint.ServerPort,
        });
        _states.Items[0] = new(this);
    }

    /// <summary>Starts timing an actual native send call, independently of tracing and logical batch composition.</summary>
    /// <remarks>Complete once after native acknowledgement or failure. Do not time outbox enqueue or message preparation.</remarks>
    public NativeSendOperation StartNativeSend() => new(_telemetry);

    /// <summary>An allocation-free timer for one native send, including asynchronous completion.</summary>
    public readonly struct NativeSendOperation
    {
        private readonly EndpointTelemetry? _telemetry;
        private readonly long _started;

        internal NativeSendOperation(EndpointTelemetry? telemetry)
        {
            _telemetry = telemetry;
            _started = telemetry is { DurationEnabled: true } ? Stopwatch.GetTimestamp() : 0;
        }

        /// <summary>Whether the duration instrument had a listener when this operation started.</summary>
        public bool IsEnabled => _started != 0;

        /// <summary>Records one duration, with bounded error.type on failure. A default timer records nothing.</summary>
        public void Complete(bool failed = false)
        {
            if (_started != 0) _telemetry!.RecordDuration(Stopwatch.GetElapsedTime(_started), failed);
        }
    }

    internal Activity? Start(ActivityContext creationContext, bool deferred) =>
        (deferred ? _create : _send).Start(new() { CreationContext = creationContext });

    internal bool IncludeBodySize => _tracing.Options.IncludeBodySize;

    /// <summary>Counts messages immediately before the adapter invokes a native send, including calls that fail.</summary>
    /// <remarks>Call after validation and preparation, once per native call with its actual message count.
    /// SendAsync and SendBatchAsync compose tracing only; they do not count native attempts.</remarks>
    public void RecordNativeSend(long count = 1) => _telemetry?.RecordSent(count);

    internal void Fail(Activity? activity, Exception exception)
    {
        MessageOperationTracing.Fail(activity, "_OTHER");
        SendLog.Failed(_logger, exception, activity?.TraceId.ToString(), activity?.SpanId.ToString());
    }

    /// <summary>Sends one serialized message, preserving existing creation headers byte for byte.</summary>
    public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken = default)
    {
        return message.TracingStarted
            ? _sendNative(message, cancellationToken)
            : Start(message, default, false, cancellationToken);
    }

    private ValueTask Start(OutgoingMessage message, ReadOnlyMemory<OutgoingMessage> messages,
        bool batch, CancellationToken cancellationToken)
    {
        // Builder.Start isolates native Activity/AsyncLocal changes without renting a runtime continuation box.
        var runner = new Runner(this, message, messages, batch, cancellationToken);
        var builder = AsyncTaskMethodBuilder.Create();
        builder.Start(ref runner);
        return runner.Result;
    }

    private ValueTask StartCore(OutgoingMessage message, ReadOnlyMemory<OutgoingMessage> messages,
        bool batch, CancellationToken cancellationToken)
    {
        State? state = null;
        Exception? error = null;
        try
        {
            if (_tracing.Source.HasListeners())
            {
                state = Rent();
                state.Ensure(batch ? messages.Length : 1);
                if (batch) messages = PrepareBatch(state, messages, cancellationToken);
                else message = PrepareSingle(state, message);
            }
            var pending = (batch ? _sendBatchNative(messages, cancellationToken) : _sendNative(message, cancellationToken))
                .ConfigureAwait(false).GetAwaiter();
            if (!pending.IsCompleted)
            {
                state ??= Rent();
                return state.Suspend(pending);
            }
            pending.GetResult();
        }
        catch (Exception exception) { error = exception; }
        // An untraced synchronous success needs neither borrowed preparation storage nor a continuation slot.
        return state is null && error is null ? default : (state ?? Rent()).FinishSynchronous(error);
    }

    private struct Runner(SendTracing owner, OutgoingMessage message, ReadOnlyMemory<OutgoingMessage> messages,
        bool batch, CancellationToken cancellationToken) : IAsyncStateMachine
    {
        internal ValueTask Result;
        public void MoveNext() => Result = owner.StartCore(message, messages, batch, cancellationToken);
        public void SetStateMachine(IAsyncStateMachine stateMachine) { }
    }

    private OutgoingMessage PrepareSingle(State state, OutgoingMessage message)
    {
        state.Activity = _send.Start(Data(message), enrich: true, client: message.IsOutboxDispatch);
        EnrichMessage(state.Activity, message);
        return message.IsOutboxDispatch ? message :
            Prepare(state, 0, message, state.Activity?.Context ?? Activity.Current?.Context ?? default);
    }

    /// <summary>Creates per-message producer spans and one native batch span, or one producer span when creation spans are disabled.</summary>
    /// <remarks>Supply a batch for this endpoint. Input order and native partial-failure behavior are unchanged.</remarks>
    public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken = default)
    {
        if (messages.IsEmpty) return _sendBatchNative(messages, cancellationToken);
        return Start(default, messages, true, cancellationToken);
    }

    private ReadOnlyMemory<OutgoingMessage> PrepareBatch(State state, ReadOnlyMemory<OutgoingMessage> messages,
        CancellationToken cancellationToken)
    {
        var linkCount = 0;
        for (var i = 0; i < messages.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var message = messages.Span[i];
            var data = Data(message);
            var context = data.CreationContext;
            if (!message.IsOutboxDispatch && _tracing.Options.CreateSpansForBatches && !HasCreationHeader(message))
            {
                using var create = _create.Start(data);
                EnrichMessage(create, message);
                context = create?.Context ?? Activity.Current?.Context ?? default;
            }
            state.Contexts[i] = context;
            if (context != default) state.Links[linkCount++] = new(context);
        }
        state.Activity = _send.Start(new() { BatchCount = messages.Length, Links = state.Links.AsMemory(0, linkCount) },
            enrich: true, client: messages.Span[0].IsOutboxDispatch);
        var fallback = state.Activity?.Context ?? Activity.Current?.Context ?? default;
        for (var i = 0; i < messages.Length; i++)
            state.Messages[i] = messages.Span[i].IsOutboxDispatch ? messages.Span[i] :
                Prepare(state, i, messages.Span[i], state.Contexts[i] != default ? state.Contexts[i] : fallback);
        return state.Messages.AsMemory(0, messages.Length);
    }

    private bool HasCreationHeader(OutgoingMessage message) => OutgoingHeaders.TryGetHeader(message, _names.TraceParent, out _);

    private MessageTraceData Data(OutgoingMessage message) => new()
    {
        CreationContext = MessageCreationContext.Read<OutgoingMessage, OutgoingHeaders>(message, _names),
    };

    private void EnrichMessage(Activity? activity, OutgoingMessage message)
    {
        if (activity is not { IsAllDataRequested: true }) return;
        activity.SetTag("messaging.message.id", Read(message, _names.Id));
        activity.SetTag("cloudevents.event_type", Read(message, _names.Type));
        activity.SetTag("cloudevents.event_source", Read(message, _names.Source));
        activity.SetTag("messaging.message.conversation_id", Read(message, _names.CorrelationId));
        if (_tracing.Options.IncludeBodySize) activity.SetTag("messaging.message.body.size", message.Body.Length);
    }

    private static string? Read(OutgoingMessage message, string name) =>
        OutgoingHeaders.TryGetHeader(message, name, out var value) ? value.ToString() : null;

    private OutgoingMessage Prepare(State state, int index, OutgoingMessage message, ActivityContext context)
    {
        if (HasCreationHeader(message) || context == default) return message.WithTracingStarted();
        var headers = state.Headers[index];
        foreach (var header in message.Headers.Span) headers.Add(header.Key, header.Value);
        MessageCreationContext.WriteIfAbsent<SendHeaders, SendHeaders>(headers, _names, context);
        return message.Forward(message.Destination, headers.Written, message.DeliverAt).WithTracingStarted();
    }

    private State Rent()
    {
        var segment = _states;
        while (true)
        {
            for (var i = 0; i < segment.Items.Length; i++)
            {
                var state = Volatile.Read(ref segment.Items[i]);
                if (state is null)
                {
                    var added = new State(this);
                    state = Interlocked.CompareExchange(ref segment.Items[i], added, null) ?? added;
                }
                if (Volatile.Read(ref state.InUse) != 0) continue;
                if (Interlocked.CompareExchange(ref state.InUse, 1, 0) != 0) continue;
                return state;
            }
            // Retain the observed concurrency high-water mark instead of dropping overflow states each burst.
            var next = Volatile.Read(ref segment.Next);
            if (next is null)
            {
                var added = new PoolSegment();
                next = Interlocked.CompareExchange(ref segment.Next, added, null) ?? added;
            }
            segment = next;
        }
    }

    private static void Return(State state)
    {
        // States stay in their slots. Publish availability only after completion is consumed.
        Volatile.Write(ref state.InUse, 0);
    }

    private sealed class PoolSegment
    {
        internal readonly State?[] Items = new State?[64];
        internal PoolSegment? Next;
    }

    private sealed class State : IValueTaskSource
    {
        private readonly SendTracing _owner;
        private readonly Action _continuation;
        private ManualResetValueTaskSourceCore<bool> _completion;
        private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _pending;
        private ExecutionContext? _executionContext;
        internal Activity? Activity;

        internal State(SendTracing owner)
        {
            _owner = owner;
            _continuation = Resume;
        }

        internal ValueTask Suspend(ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter pending)
        {
            _pending = pending;
            var result = new ValueTask(this, _completion.Version);
            _executionContext = ExecutionContext.Capture();
            _pending.UnsafeOnCompleted(_continuation);
            return result;
        }

        internal ValueTask FinishSynchronous(Exception? error)
        {
            error = Finish(error);
            if (error is null)
            {
                Return(this);
                return default;
            }
            // Keep synchronous cancellation status and the original exception, without a Task wrapper.
            var failed = new ValueTask(this, _completion.Version);
            _completion.SetException(error);
            return failed;
        }

        private void Resume()
        {
            var context = _executionContext;
            _executionContext = null;
            if (context is not null)
                ExecutionContext.Run(context, static state => ((State)state!).Complete(), this);
            else
            {
                var runner = new CompletionRunner(this);
                var builder = AsyncTaskMethodBuilder.Create();
                builder.Start(ref runner);
            }
        }

        private void Complete()
        {
            Exception? error = null;
            try { _pending.GetResult(); }
            catch (Exception exception) { error = exception; }
            error = Finish(error);
            // Clear borrowed references before an inline caller continuation can consume and reuse this slot.
            if (error is null) _completion.SetResult(true);
            else _completion.SetException(error);
        }

        private Exception? Finish(Exception? error)
        {
            try
            {
                try
                {
                    if (error is not null) _owner.Fail(Activity, error);
                }
                finally { Activity?.Dispose(); }
            }
            catch (Exception exception) { error = exception; }
            finally
            {
                Activity = null;
                _pending = default;
                _executionContext = null;
                Clear();
            }
            return error;
        }

        public void GetResult(short token)
        {
            // Completion alone does not release the slot: its version belongs to the awaiting caller.
            try { _completion.GetResult(token); }
            finally
            {
                // Drop the caller continuation and ExecutionContext before this retained slot becomes idle.
                _completion.Reset();
                Return(this);
            }
        }

        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _completion.OnCompleted(continuation, state, token, flags);

        private readonly struct CompletionRunner(State state) : IAsyncStateMachine
        {
            public void MoveNext() => state.Complete();
            public void SetStateMachine(IAsyncStateMachine stateMachine) { }
        }

        internal int InUse;
        internal OutgoingMessage[] Messages = [];
        internal ActivityContext[] Contexts = [];
        internal ActivityLink[] Links = [];
        internal SendHeaders[] Headers = [];
        private int _count;

        internal void Ensure(int count)
        {
            if (Messages.Length < count)
            {
                Array.Resize(ref Messages, count);
                Array.Resize(ref Contexts, count);
                Array.Resize(ref Links, count);
                var previous = Headers.Length;
                Array.Resize(ref Headers, count);
                for (var i = previous; i < count; i++) Headers[i] = new();
            }
            _count = count;
        }

        internal void Clear()
        {
            Array.Clear(Messages, 0, _count);
            Array.Clear(Contexts, 0, _count);
            Array.Clear(Links, 0, _count);
            for (var i = 0; i < _count; i++) Headers[i].Clear();
            _count = 0;
        }
    }

    private readonly struct OutgoingHeaders : IMessageHeaderReader<OutgoingMessage>
    {
        public static bool TryGetHeader(OutgoingMessage message, string name, out WireString value)
        {
            foreach (var header in message.Headers.Span)
                if (header.Key == name) { value = header.Value; return true; }
            value = default;
            return false;
        }
    }
}

internal static partial class SendLog
{
    [LoggerMessage(2, LogLevel.Error, "Message send failed. TraceId={TraceId} SpanId={SpanId}.")]
    internal static partial void Failed(ILogger logger, Exception exception, string? traceId, string? spanId);
}
