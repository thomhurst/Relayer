using System.Threading.Tasks.Sources;
using NATS.Client.JetStream.Models;

namespace Relayer.Nats.Tests;

public class NatsSendOperationTests
{
    [Test]
    public async Task Cancelled_native_publication_returns_the_operation_after_observation()
    {
        var pool = new NatsNativeTransport.Endpoint(null!, false);
        var operation = pool.Rent();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var send = operation.Track(ValueTask.FromCanceled<PubAckResponse>(cancellation.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await send);
        await Assert.That(ReferenceEquals(pool.Rent(), operation)).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Native_completion_keeps_state_until_consumed_and_resets_after_success_or_failure(bool fail)
    {
        var pool = new NatsNativeTransport.Endpoint(null!, false);
        var operation = pool.Rent();
        var native = new TaskCompletionSource<PubAckResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        operation.Headers["ce-id"] = "original";
        var send = operation.Track(new(native.Task));
        await Assert.That(send.IsCompleted).IsFalse();
        await Assert.ThrowsAsync<InvalidOperationException>(() => { send.GetAwaiter().GetResult(); return Task.CompletedTask; });
        if (fail) native.SetException(new IOException("native publication failed"));
        else native.SetResult(new PubAckResponse { Stream = "events", Seq = 1 });
        if (fail) await Assert.ThrowsAsync<IOException>(async () => await send);
        else await send;
        await Assert.That(operation.Headers.Count).IsEqualTo(0);
        var next = pool.Rent();
        await Assert.That(ReferenceEquals(next, operation)).IsTrue();
        // A stale consumer must not reset/return this new pending publication.
        var secondNative = new TaskCompletionSource<PubAckResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = next.Track(new(secondNative.Task));
        await Assert.ThrowsAsync<InvalidOperationException>(() => { send.GetAwaiter().GetResult(); return Task.CompletedTask; });
        await Assert.That(second.IsCompleted).IsFalse();
        secondNative.SetResult(new PubAckResponse { Stream = "events", Seq = 2 });
        await second;
    }

    [Test]
    public async Task Cross_thread_completion_reuses_state_without_allocating()
    {
        var pool = new NatsNativeTransport.Endpoint(null!, false);
        using var ready = new AutoResetEvent(false);
        using var completed = new AutoResetEvent(false);
        var native = new NativeCompletion();
        var measuring = false;
        long workerAdded = 0;
        var worker = new Thread(() =>
        {
            while (ready.WaitOne())
            {
                if (native.Stopping) return;
                var before = GC.GetAllocatedBytesForCurrentThread();
                native.Complete();
                if (measuring) workerAdded += GC.GetAllocatedBytesForCurrentThread() - before;
                completed.Set();
            }
        });
        worker.Start();
        long added;
        try
        {
            // Warm both threads and the endpoint slots before measuring initiating-thread
            // allocations. Completion happens on a different, fixed thread every time.
            for (var i = 0; i < 2000; i++) Send();
            measuring = true;
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 2000; i++) Send();
            added = GC.GetAllocatedBytesForCurrentThread() - before;
        }
        finally
        {
            native.Stopping = true;
            ready.Set();
            worker.Join();
        }
        await Assert.That(added).IsEqualTo(0L);
        await Assert.That(workerAdded).IsEqualTo(0L);

        void Send()
        {
            var operation = pool.Rent();
            var send = operation.Track(native.Next());
            ready.Set();
            completed.WaitOne();
            send.GetAwaiter().GetResult();
        }
    }

    private sealed class NativeCompletion : IValueTaskSource<PubAckResponse>
    {
        private ManualResetValueTaskSourceCore<PubAckResponse> _source;
        private readonly PubAckResponse _ack = new() { Stream = "events", Seq = 1 };
        internal bool Stopping;
        internal ValueTask<PubAckResponse> Next() => new(this, _source.Version);
        internal void Complete() => _source.SetResult(_ack);
        public PubAckResponse GetResult(short token)
        {
            try { return _source.GetResult(token); }
            finally { _source.Reset(); }
        }
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
    }
}
