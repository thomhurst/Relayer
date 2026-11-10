using System.Collections.Concurrent;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Relayer.GooglePubSub.Tests;

// Pass-through RPC instrumentation: real client streams, broker acknowledgements and disposal.
internal sealed class PubSubShutdownInterceptor : Interceptor
{
    private readonly ConcurrentQueue<string> _violations = new();
    private readonly ConcurrentDictionary<string, byte> _acks = new();
    internal IOException? InitialWriteFailure;
    private volatile TaskCompletionSource _initialWrite = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal Task WaitForInitialWriteAsync(CancellationToken token) => _initialWrite.Task.WaitAsync(token);
    private int _streams;
    private int _closedStreams;
    private int _publisherClosed;
    private int _publishing;
    private int _receiverClosed;
    private int _settling;

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context, AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        if (request is PublishRequest)
        {
            if (Volatile.Read(ref _publisherClosed) != 0) _violations.Enqueue("Publish after publisher closure");
            Interlocked.Increment(ref _publishing);
            AsyncUnaryCall<TResponse> call;
            try { call = continuation(request, context); }
            catch { Interlocked.Decrement(ref _publishing); throw; }
            return new(ObservePublishAsync(call.ResponseAsync), call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
        }
        if (request is AcknowledgeRequest or ModifyAckDeadlineRequest)
        {
            if (Volatile.Read(ref _receiverClosed) != 0) _violations.Enqueue("Ack or renewal after native subscriber closure");
            Interlocked.Increment(ref _settling);
            AsyncUnaryCall<TResponse> call;
            try { call = continuation(request, context); }
            catch { Interlocked.Decrement(ref _settling); throw; }
            return new(ObserveSettlementAsync(call.ResponseAsync, request), call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
        }
        return continuation(request, context);
    }

    private async Task<T> ObservePublishAsync<T>(Task<T> response)
    {
        try { return await response; }
        finally { Interlocked.Decrement(ref _publishing); }
    }

    private async Task<T> ObserveSettlementAsync<T>(Task<T> response, object request)
    {
        try
        {
            var result = await response;
            if (request is AcknowledgeRequest ack)
                foreach (var id in ack.AckIds) _acks.TryAdd(id, 0);
            return result;
        }
        finally { Interlocked.Decrement(ref _settling); }
    }

    internal void ReceiverClosed()
    {
        Interlocked.Exchange(ref _receiverClosed, 1);
        _initialWrite = new(TaskCreationOptions.RunContinuationsAsynchronously);
        if (Volatile.Read(ref _settling) != 0) _violations.Enqueue("Subscriber closed before settlement RPCs completed");
    }

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context, AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
    {
        var call = continuation(context);
        if (typeof(TRequest) != typeof(StreamingPullRequest)) return call;
        Interlocked.Increment(ref _streams);
        var writer = new Writer<TRequest>(call.RequestStream, this);
        return new(writer, call.ResponseStream, call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, () =>
        {
            writer.Close();
            call.Dispose();
        });
    }

    internal void PublisherClosed()
    {
        Interlocked.Exchange(ref _publisherClosed, 1);
        if (Volatile.Read(ref _publishing) != 0) _violations.Enqueue("Publisher closed before admitted RPCs completed");
    }

    internal void AssertClean(bool requireClosure = false)
    {
        if (!_violations.IsEmpty) throw new InvalidOperationException(string.Join("; ", _violations));
        if (_acks.Count != 1) throw new InvalidOperationException($"Expected one broker-confirmed native Ack, observed {_acks.Count}.");
        if (Volatile.Read(ref _streams) == 0) throw new InvalidOperationException("No real StreamingPull RPC was instrumented.");
        if (_receiverClosed == 0) throw new InvalidOperationException("Native subscriber stop was not observed.");
        if (requireClosure) AssertPublisherClosed();
        if (requireClosure && _closedStreams != _streams)
            throw new InvalidOperationException("Native Pub/Sub streams or publisher remained open after disposal.");
    }

    internal void AssertPublisherClosed()
    {
        if (Volatile.Read(ref _publisherClosed) == 0)
            throw new InvalidOperationException("Native Pub/Sub publisher remained open after disposal.");
    }

    private sealed class Writer<T>(IClientStreamWriter<T> native, PubSubShutdownInterceptor owner) : IClientStreamWriter<T>
    {
        private int _closed;
        public WriteOptions? WriteOptions { get => native.WriteOptions; set => native.WriteOptions = value; }
        public async Task WriteAsync(T message)
        {
            if (Volatile.Read(ref _closed) != 0) owner._violations.Enqueue("Ack, renewal or receive request after stream closure");
            if (message is StreamingPullRequest { Subscription.Length: > 0 }
                && Interlocked.Exchange(ref owner.InitialWriteFailure, null) is { } failure) throw failure;
            await native.WriteAsync(message);
            if (message is StreamingPullRequest { Subscription.Length: > 0 }) owner._initialWrite.TrySetResult();
            if (message is StreamingPullRequest pull)
                foreach (var ack in pull.AckIds) owner._acks.TryAdd(ack, 0);
        }
        public async Task CompleteAsync()
        {
            await native.CompleteAsync();
            Close();
        }
        internal void Close()
        {
            if (Interlocked.Exchange(ref _closed, 1) == 0) Interlocked.Increment(ref owner._closedStreams);
        }
    }
}
