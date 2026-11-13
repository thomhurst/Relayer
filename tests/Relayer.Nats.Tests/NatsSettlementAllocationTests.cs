using Relayer.Transports;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace Relayer.Nats.Tests;

public class NatsSettlementAllocationTests
{
    [Test]
    public async Task Confirmed_settlement_reuses_state_across_threads()
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var worker = new NatsConsumerWorker("events", "worker", new(), 1, tracing);
        // Disable external test-runner listeners for this allocation pin.
        tracing.Source.Dispose();
        using var ready = new AutoResetEvent(false);
        using var completed = new AutoResetEvent(false);
        var native = new NativeCompletion();
        var measuring = false;
        long workerAdded = 0;
        var thread = new Thread(() =>
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
        thread.Start();
        long added;
        try
        {
            for (var i = 0; i < 2000; i++) Settle();
            measuring = true;
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 2000; i++) Settle();
            added = GC.GetAllocatedBytesForCurrentThread() - before;
        }
        finally { native.Stopping = true; ready.Set(); thread.Join(); }
        await Assert.That(added).IsEqualTo(0L);
        await Assert.That(workerAdded).IsEqualTo(0L);

        void Settle()
        {
            var settlement = worker.SettleAsync(new(native, null!), Disposition.Complete, default);
            ready.Set();
            completed.WaitOne();
            settlement.GetAwaiter().GetResult();
        }
    }

    private sealed class NativeCompletion : INatsJSMsg<ReadOnlyMemory<byte>>, IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _source;
        internal bool Stopping;
        internal void Complete() => _source.SetResult(true);
        public ValueTask AckAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) => new(this, _source.Version);
        public void GetResult(short token) { try { _source.GetResult(token); } finally { _source.Reset(); } }
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
        public NatsHeaders? Headers => null;
        public string Subject => "events";
        public int Size => 0;
        public ReadOnlyMemory<byte> Data => default;
        public INatsConnection? Connection => null;
        public NatsJSMsgMetadata? Metadata => null;
        public string? ReplyTo => null;
        public NatsException? Error => null;
        public void EnsureSuccess() { }
        public ValueTask ReplyAsync(NatsHeaders? headers = null, string? replyTo = null, NatsPubOpts? opts = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask NakAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask AckProgressAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask AckTerminateAsync(AckOpts? opts = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
