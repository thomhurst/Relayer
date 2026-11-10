using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Transports;

namespace Relayer.Nats;

// Retain continuation state with the endpoint instead of relying on the runtime builder's
// thread-local cache. A synchronously driven caller rents and completes on different threads.
internal sealed class NatsSendOperation : IValueTaskSource
{
    private readonly NatsNativeTransport.Endpoint _pool;
    private readonly Action _complete;
    private ManualResetValueTaskSourceCore<bool> _completion;
    private ConfiguredValueTaskAwaitable<PubAckResponse>.ConfiguredValueTaskAwaiter _pending;
    private SendTracing.NativeSendOperation _duration;
    private ExecutionContext? _context;

    internal NatsSendOperation(NatsNativeTransport.Endpoint pool, bool caseSensitive)
    {
        _pool = pool;
        Headers = new(caseSensitive);
        _complete = Resume;
    }

    internal NatsPublishHeaders Headers { get; }

    internal ValueTask Publish(NatsJSContext context, OutgoingMessage message, NatsJSPubOpts? options,
        CancellationToken token, SendTracing.NativeSendOperation duration)
    {
        _duration = duration;
        var version = _completion.Version;
        try
        {
            return Track(context.PublishAsync(message.Destination, message.Body, Headers,
                opts: options, headers: Headers, cancellationToken: token), duration);
        }
        catch (Exception error) { Finish(error); }
        return new(this, version);
    }

    internal ValueTask Track(ValueTask<PubAckResponse> pending, SendTracing.NativeSendOperation duration = default)
    {
        _duration = duration;
        _context = ExecutionContext.Capture();
        var version = _completion.Version;
        _pending = pending.ConfigureAwait(false).GetAwaiter();
        if (_pending.IsCompleted) Complete();
        else _pending.UnsafeOnCompleted(_complete);
        return new(this, version);
    }

    private void Resume()
    {
        if (_context is { } context)
            ExecutionContext.Run(context, static state => ((NatsSendOperation)state!).Complete(), this);
        else Complete();
    }

    private void Complete()
    {
        Exception? failure = null;
        try { _pending.GetResult().EnsureSuccess(); }
        catch (Exception error) { failure = error; }
        Finish(failure);
    }

    private void Finish(Exception? failure)
    {
        _pending = default;
        _context = null;
        var duration = _duration;
        _duration = default;
        Headers.Reset();
        try { duration.Complete(failure is not null); }
        catch (Exception error) { failure = error; }
        // Completion can run the caller inline, returning this object and starting another
        // publication. Do not access any operation fields after publishing the result.
        if (failure is null) _completion.SetResult(true);
        else _completion.SetException(failure);
    }

    public void GetResult(short token)
    {
        // A premature/stale consumer must not return an active operation to the pool.
        if (_completion.GetStatus(token) == ValueTaskSourceStatus.Pending)
            throw new InvalidOperationException("The NATS publication has not completed.");
        try { _completion.GetResult(token); }
        finally
        {
            _completion.Reset();
            _pool.Return(this);
        }
    }

    public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _completion.OnCompleted(continuation, state, token, flags);
}
