using Relayer.Transports;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using System.Threading.Tasks.Sources;
using RabbitMQ.Client;

namespace Relayer.RabbitMQ;

// Unlike the runtime's capped async-builder cache, retain operations at the observed concurrency high-water mark.
internal sealed class RabbitMqSendPool
{
    private readonly Page _first = new();
    internal RabbitMqSendOperation Rent()
    {
        for (Page? page = _first; page is not null; page = Volatile.Read(ref page.Next))
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.Exchange(ref page.Slots[i], null) is { } operation) return operation;
        return new(this);
    }
    internal void Return(RabbitMqSendOperation operation)
    {
        var page = _first;
        while (true)
        {
            for (var i = 0; i < page.Slots.Length; i++)
                if (Interlocked.CompareExchange(ref page.Slots[i], operation, null) is null) return;
            var next = Volatile.Read(ref page.Next);
            if (next is null)
            {
                var added = new Page();
                next = Interlocked.CompareExchange(ref page.Next, added, null) ?? added;
            }
            page = next;
        }
    }
    private sealed class Page
    {
        internal readonly RabbitMqSendOperation?[] Slots = new RabbitMqSendOperation?[64];
        internal Page? Next;
    }
}

internal sealed class RabbitMqSendOperation : IValueTaskSource
{
    private readonly RabbitMqSendPool _pool;
    private readonly Action _continuation;
    private ManualResetValueTaskSourceCore<bool> _completion;
    private ConfiguredValueTaskAwaitable<RabbitMqPublisher>.ConfiguredValueTaskAwaiter _lease;
    private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _pending;
    private ExecutionContext? _context;
    private Channel<RabbitMqPublisher> _publishers = null!;
    private IConnection _connection = null!;
    private RabbitMqNativeTransport _transport = null!;
    private RabbitMqSendEndpoint _endpoint = null!;
    private RabbitMqPublisher? _publisher;
    private OutgoingMessage _message;
    private ReadOnlyMemory<OutgoingMessage> _batch;
    private int _batchIndex;
    private Exception? _batchError;
    private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter[] _batchSends = [];
    private int _batchCount;
    private bool _borrowed;
    private bool _traceBatchMessages;
    private CancellationToken _token;
    private long _delay;
    private long _started;
    private Stage _stage;
    private int _consumed;
    private enum Stage { Lease, Recover, Publish, BatchLease, BatchRecover, BatchDrain }

    internal RabbitMqSendOperation(RabbitMqSendPool pool)
    {
        _pool = pool;
        _continuation = Resume;
        _completion.Reset();
    }

    internal ValueTask Start(RabbitMqNativeTransport transport, RabbitMqSendEndpoint endpoint, OutgoingMessage message,
        long delay, IConnection connection, Channel<RabbitMqPublisher> publishers, CancellationToken token)
    {
        _consumed = 0;
        _borrowed = false;
        _context = ExecutionContext.Capture();
        _transport = transport;
        _endpoint = endpoint;
        _message = message;
        _delay = delay;
        _connection = connection;
        _publishers = publishers;
        _token = token;
        _stage = Stage.Lease;
        try
        {
            _lease = publishers.Reader.ReadAsync(token).ConfigureAwait(false).GetAwaiter();
            if (_lease.IsCompleted) Advance();
            else _lease.UnsafeOnCompleted(_continuation);
        }
        catch (Exception error) { Finish(error); }
        return new(this, _completion.Version);
    }

    internal ValueTask StartBorrowed(RabbitMqNativeTransport transport, RabbitMqSendEndpoint endpoint,
        RabbitMqPublisher publisher, OutgoingMessage message, CancellationToken token)
    {
        _consumed = 0;
        _borrowed = true;
        _context = ExecutionContext.Capture();
        _transport = transport;
        _endpoint = endpoint;
        _publisher = publisher;
        _message = message;
        _token = token;
        _started = Stopwatch.GetTimestamp();
        _stage = Stage.Recover;
        _pending = ValueTask.CompletedTask.ConfigureAwait(false).GetAwaiter();
        Advance();
        return new(this, _completion.Version);
    }

    internal ValueTask StartBatch(RabbitMqNativeTransport transport, ReadOnlyMemory<OutgoingMessage> messages,
        IConnection connection, Channel<RabbitMqPublisher> publishers, CancellationToken token, bool traceMessages)
    {
        _consumed = 0;
        _borrowed = false;
        _context = ExecutionContext.Capture();
        _transport = transport;
        _connection = connection;
        _publishers = publishers;
        _batch = messages;
        _batchIndex = 0;
        _batchCount = 0;
        _batchError = null;
        _traceBatchMessages = traceMessages;
        _token = token;
        _stage = Stage.BatchLease;
        try
        {
            _lease = publishers.Reader.ReadAsync(token).ConfigureAwait(false).GetAwaiter();
            if (_lease.IsCompleted) Advance();
            else _lease.UnsafeOnCompleted(_continuation);
        }
        catch (Exception error) { Finish(error); }
        return new(this, _completion.Version);
    }

    private void Resume()
    {
        if (_context is null) Advance();
        else ExecutionContext.Run(_context, static state => ((RabbitMqSendOperation)state!).Advance(), this);
    }

    private void Advance()
    {
        try
        {
            while (true)
            {
                switch (_stage)
                {
                    case Stage.BatchLease:
                        _publisher = _lease.GetResult();
                        _lease = default;
                        _stage = Stage.BatchRecover;
                        _pending = (_publisher.Channel.IsOpen ? ValueTask.CompletedTask :
                            _transport.ReplacePublisherChannelAsync(_publisher, _connection, _token)).ConfigureAwait(false).GetAwaiter();
                        if (!_pending.IsCompleted) { _pending.UnsafeOnCompleted(_continuation); return; }
                        break;
                    case Stage.BatchRecover:
                        _pending.GetResult();
                        _pending = default;
                        _batchSends = _publisher!.BatchSends(_batch.Length);
                        // RabbitMQ.Client 7.2.2 releases its publish semaphore before awaiting native confirms.
                        // Each slot owns its mutable properties until its native call completes. Native flow control
                        // can suspend before serialization, so reusing the leased publisher's properties is unsafe.
                        try
                        {
                            for (var i = 0; i < _batch.Length; i++)
                            {
                                _token.ThrowIfCancellationRequested();
                                var send = _transport.SendBatchItemAsync(_batch.Span[i], _publisher!.BatchSlot(i),
                                    _token, _traceBatchMessages).ConfigureAwait(false).GetAwaiter();
                                _batchSends[_batchCount++] = send;
                            }
                        }
                        catch (Exception error) { _batchError = error; }
                        _stage = Stage.BatchDrain;
                        break;
                    case Stage.BatchDrain:
                        while (_batchIndex < _batchCount)
                        {
                            ref var send = ref _batchSends[_batchIndex];
                            if (!send.IsCompleted) { send.UnsafeOnCompleted(_continuation); return; }
                            try { send.GetResult(); }
                            catch (Exception error) { _batchError ??= error; }
                            send = default;
                            _batchIndex++;
                        }
                        Finish(_batchError);
                        return;
                    case Stage.Lease:
                        _publisher = _lease.GetResult();
                        _lease = default;
                        _started = Stopwatch.GetTimestamp();
                        _stage = Stage.Recover;
                        _pending = (_publisher.Channel.IsOpen ? ValueTask.CompletedTask :
                            _transport.ReplacePublisherChannelAsync(_publisher, _connection, _token)).ConfigureAwait(false).GetAwaiter();
                        if (!_pending.IsCompleted) { _pending.UnsafeOnCompleted(_continuation); return; }
                        break;
                    case Stage.Recover:
                        _pending.GetResult();
                        // Channel contention and recovery must not extend an absolute delivery deadline.
                        _delay = _endpoint.Delay(_message.DeliverAt);
                        _publisher!.Prepare(_message, _delay);
                        _stage = Stage.Publish;
                        _endpoint.Telemetry?.RecordSent();
                        _pending = _publisher.Channel.BasicPublishAsync(_delay > 0 ? _endpoint.DelayedExchange! : _endpoint.Exchange,
                            _endpoint.RoutingKey, mandatory: _delay == 0, _publisher.Properties, _message.Body, _token)
                            .ConfigureAwait(false).GetAwaiter();
                        if (!_pending.IsCompleted) { _pending.UnsafeOnCompleted(_continuation); return; }
                        break;
                    case Stage.Publish:
                        _pending.GetResult();
                        Finish(null);
                        return;
                }
            }
        }
        catch (Exception error) { Finish(error); }
    }

    private void Finish(Exception? error)
    {
        if (_publisher is { } publisher)
        {
            try { _endpoint?.Telemetry?.RecordDuration(Stopwatch.GetElapsedTime(_started), error is not null); }
            catch (Exception telemetryError) { error ??= telemetryError; }
            finally
            {
                publisher.Clear();
                _publisher = null;
                if (!_borrowed) _publishers.Writer.TryWrite(publisher);
            }
        }
        _message = default;
        _batch = default;
        _batchError = null;
        _batchSends = [];
        _pending = default;
        _lease = default;
        if (error is null) _completion.SetResult(true);
        else _completion.SetException(error);
    }

    public void GetResult(short token)
    {
        if (_completion.GetStatus(token) == ValueTaskSourceStatus.Pending)
            throw new InvalidOperationException("The RabbitMQ send has not completed.");
        if (Interlocked.Exchange(ref _consumed, 1) != 0)
            throw new InvalidOperationException("Consume the RabbitMQ send ValueTask only once.");
        try { _completion.GetResult(token); }
        finally
        {
            _context = null;
            _transport = null!;
            _connection = null!;
            _publishers = null!;
            _endpoint = null!;
            _token = default;
            // Detach the caller's continuation and any exception before retaining this operation.
            _completion.Reset();
            _pool.Return(this);
        }
    }
    public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _completion.OnCompleted(continuation, state, token, flags);
}
