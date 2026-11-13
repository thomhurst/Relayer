using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;

namespace Relayer.TestFixtures;

// A necessary-condition experiment, not an implementation of a message pipeline. The owned
// operation models the completion storage already attached to a rented context/state. Native
// sources, scheduling, overlap and GetResult ownership are identical for every variant.
public sealed class PooledBuilderCompletionFixture : IDisposable
{
    private readonly Source[] _sources;
    private readonly OwnedOperation[] _operations;
    private readonly ValueTask[] _pending;
    private readonly ValueTask<int>[] _results;
    private readonly AutoResetEvent[] _release = [new(false), new(false)];
    private readonly AutoResetEvent _finished = new(false);
    private readonly Thread[] _workers;
    private readonly AsyncLocal<string?> _ambient = new();
    private bool _stopping;
    private int _worker;
    private Exception? _error;
    private long _workerBytes;
    private bool _workerContextRestored;

    public enum Mode { Native, Owned, PooledVoid, PooledResult }
    public int Count => _sources.Length;
    public bool Asynchronous { get; }
    public long CallerBytes { get; private set; }
    public long WorkerBytes => _workerBytes;
    public bool ContextRestored { get; private set; }

    public PooledBuilderCompletionFixture(int count, bool asynchronous, bool ambientContext)
    {
        Asynchronous = asynchronous;
        _sources = new Source[count];
        _operations = new OwnedOperation[count];
        _pending = new ValueTask[count];
        _results = new ValueTask<int>[count];
        for (var i = 0; i < count; i++)
        {
            _sources[i] = new Source(_ambient);
            _operations[i] = new OwnedOperation();
        }
        if (ambientContext) _ambient.Value = "caller";
        _workers = [new(() => Work(0)), new(() => Work(1))];
        // Do not let thread startup inherit the caller's ambient context.
        if (ExecutionContext.IsFlowSuppressed()) StartWorkers();
        else { using (ExecutionContext.SuppressFlow()) StartWorkers(); }
    }

    private void StartWorkers()
    {
        foreach (var thread in _workers) { thread.IsBackground = true; thread.Start(); }
    }

    public long Run(Mode mode, Exception? error = null)
    {
        _error = error;
        var expected = _ambient.Value;
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Count; i++)
        {
            var native = _sources[i].Start(!Asynchronous, error);
            if (mode == Mode.PooledResult) _results[i] = AwaitResult(native);
            else _pending[i] = mode switch
            {
                Mode.Native => native,
                Mode.Owned => _operations[i].Start(native),
                _ => AwaitVoid(native),
            };
        }
        if (Asynchronous)
        {
            _release[_worker].Set();
            _worker ^= 1;
            if (!_finished.WaitOne(TimeSpan.FromSeconds(30))) throw new TimeoutException("Completion worker did not finish.");
        }
        Exception? failure = null;
        for (var i = 0; i < Count; i++)
        {
            try
            {
                if (mode == Mode.PooledResult)
                {
                    if (!_results[i].IsCompleted) throw new InvalidOperationException("Result did not complete.");
                    if (_results[i].GetAwaiter().GetResult() != 1) throw new InvalidOperationException("Wrong result.");
                }
                else
                {
                    if (!_pending[i].IsCompleted) throw new InvalidOperationException("Operation did not complete.");
                    _pending[i].GetAwaiter().GetResult();
                }
            }
            catch (Exception exception) { failure ??= exception; }
            _pending[i] = default;
            _results[i] = default;
        }
        CallerBytes = GC.GetAllocatedBytesForCurrentThread() - start;
        // Native GetResult runs on the caller; wrappers must resume in the captured caller context.
        ContextRestored = _ambient.Value == expected && (!Asynchronous || _workerContextRestored);
        foreach (var source in _sources) ContextRestored &= source.Observed == expected;
        _error = null;
        if (failure is not null) ExceptionDispatchInfo.Throw(failure);
        return CallerBytes + (Asynchronous ? _workerBytes : 0);
    }

#if NET11_0_OR_GREATER
    [RuntimeAsyncMethodGeneration(false)]
#endif
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private static async ValueTask AwaitVoid(ValueTask pending) => await pending.ConfigureAwait(false);

#if NET11_0_OR_GREATER
    [RuntimeAsyncMethodGeneration(false)]
#endif
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<int> AwaitResult(ValueTask pending)
    {
        await pending.ConfigureAwait(false);
        return 1;
    }

    private void Work(int index)
    {
        _ambient.Value = "worker";
        while (true)
        {
            _release[index].WaitOne();
            if (_stopping) return;
            var start = GC.GetAllocatedBytesForCurrentThread();
            foreach (var source in _sources) source.Complete(_error);
            _workerBytes = GC.GetAllocatedBytesForCurrentThread() - start;
            _workerContextRestored = _ambient.Value == "worker";
            _finished.Set();
        }
    }

    public void Dispose()
    {
        _stopping = true;
        foreach (var release in _release) release.Set();
        foreach (var worker in _workers) worker.Join();
        foreach (var release in _release) release.Dispose();
        _finished.Dispose();
        _ambient.Value = null;
    }

    private sealed class Source(AsyncLocal<string?> ambient) : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _completion;
        internal string? Observed;
        internal ValueTask Start(bool synchronous, Exception? error)
        {
            _completion.Reset();
            Observed = null;
            if (synchronous) Complete(error);
            return new(this, _completion.Version);
        }
        internal void Complete(Exception? error)
        {
            if (error is null) _completion.SetResult(true);
            else _completion.SetException(error);
        }
        public void GetResult(short token) { Observed = ambient.Value; _completion.GetResult(token); }
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _completion.OnCompleted(continuation, state, token, flags);
    }

    private sealed class OwnedOperation : IValueTaskSource
    {
        private readonly Action _continuation;
        private ManualResetValueTaskSourceCore<bool> _completion;
        private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _pending;
        private ExecutionContext? _context;
        internal OwnedOperation() => _continuation = Resume;
        internal ValueTask Start(ValueTask native)
        {
            _pending = native.ConfigureAwait(false).GetAwaiter();
            if (_pending.IsCompleted)
            {
                try { _pending.GetResult(); return default; }
                catch (Exception error) { return ValueTask.FromException(error); }
                finally { _pending = default; }
            }
            _context = ExecutionContext.Capture();
            _pending.UnsafeOnCompleted(_continuation);
            return new(this, _completion.Version);
        }
        private void Resume()
        {
            var context = _context;
            _context = null;
            if (context is null) Complete();
            else ExecutionContext.Run(context, static state => ((OwnedOperation)state!).Complete(), this);
        }
        private void Complete()
        {
            Exception? error = null;
            try { _pending.GetResult(); }
            catch (Exception exception) { error = exception; }
            _pending = default;
            if (error is null) _completion.SetResult(true);
            else _completion.SetException(error);
        }
        public void GetResult(short token)
        {
            try { _completion.GetResult(token); }
            finally { _completion.Reset(); }
        }
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _completion.OnCompleted(continuation, state, token, flags);
    }
}
