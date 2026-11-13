using System.Diagnostics;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.Options;

namespace Relayer.TestFixtures;

// The native sink retains its completion sources and completes on a dedicated thread. Both tests and
// benchmarks exercise the same burst, including wrapper suspension rather than only native entry.
public sealed class SendTracingCompletionFixture : IDisposable
{
    public const int Count = 128;
    private readonly MessageTracing _tracing = new(Options.Create(new TracingOptions()), []);
    private readonly ActivityListener? _listener;
    private readonly Activity _ambient = new Activity("request").Start();
    private readonly SendTracing _send;
    private readonly Source[] _sources = new Source[Count];
    private readonly ValueTask[] _pending = new ValueTask[Count];
    private readonly OutgoingMessage[] _messages = [new("orders", new byte[] { 1 }), new("orders", new byte[] { 2 })];
    private readonly AutoResetEvent _release = new(false);
    private readonly AutoResetEvent _finished = new(false);
    private readonly Thread _worker;
    private readonly bool _batch;
    private bool _stopping;
    private int _next;
    private long _workerBytes;

    public long CallerBytes { get; private set; }
    public long WorkerBytes => _workerBytes;

    public SendTracingCompletionFixture(bool batch, bool listener)
    {
        _batch = batch;
        if (listener)
        {
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, _tracing.Source),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None,
            };
            ActivitySource.AddActivityListener(_listener);
        }
        for (var i = 0; i < Count; i++) _sources[i] = new();
        _send = new(_tracing, new() { System = "test.native", Destination = "orders" }, Send, SendBatch);
        _worker = new(Work) { IsBackground = true };
        _worker.Start();
    }

    // Async consumer allocation belongs to the test harness and is created outside the counters.
    // The benchmark uses a synchronous outer caller, matching BenchmarkDotNet's calling pattern.
    public long Run(bool traced, bool asynchronousCaller = false)
    {
        _next = 0;
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Count; i++)
        {
            _pending[i] = traced
                ? _batch ? _send.SendBatchAsync(_messages) : _send.SendAsync(_messages[0])
                : _batch ? SendBatch(_messages, default) : Send(_messages[0], default);
            if (_pending[i].IsCompleted) throw new InvalidOperationException("Native send must suspend.");
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - start;
        var consumer = asynchronousCaller ? AwaitAll() : null;
        start = GC.GetAllocatedBytesForCurrentThread();
        _release.Set();
        if (!_finished.WaitOne(TimeSpan.FromSeconds(30))) throw new TimeoutException("Native completion did not finish.");
        if (consumer is not null)
        {
            if (!consumer.IsCompleted) throw new InvalidOperationException("Async caller did not finish.");
            consumer.GetAwaiter().GetResult();
        }
        else
        {
            for (var i = 0; i < Count; i++)
            {
                if (!_pending[i].IsCompleted) throw new InvalidOperationException("Send did not complete.");
                _pending[i].GetAwaiter().GetResult();
            }
        }
        CallerBytes = allocated + GC.GetAllocatedBytesForCurrentThread() - start;
        return CallerBytes + _workerBytes;
    }

    private async Task AwaitAll()
    {
        for (var i = 0; i < Count; i++) await _pending[i].ConfigureAwait(false);
    }

    private ValueTask Send(OutgoingMessage message, CancellationToken token) => _sources[_next++].Wait();
    private ValueTask SendBatch(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => _sources[_next++].Wait();

    private void Work()
    {
        while (true)
        {
            _release.WaitOne();
            if (_stopping) return;
            var start = GC.GetAllocatedBytesForCurrentThread();
            foreach (var source in _sources) source.Complete();
            _workerBytes = GC.GetAllocatedBytesForCurrentThread() - start;
            _finished.Set();
        }
    }

    public void Dispose()
    {
        _stopping = true;
        _release.Set();
        _worker.Join();
        _release.Dispose();
        _finished.Dispose();
        _listener?.Dispose();
        _tracing.Dispose();
        _ambient.Dispose();
    }

    private sealed class Source : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _completion;
        internal ValueTask Wait() { _completion.Reset(); return new(this, _completion.Version); }
        internal void Complete() => _completion.SetResult(true);
        public void GetResult(short token) => _completion.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _completion.OnCompleted(continuation, state, token, flags);
    }
}
