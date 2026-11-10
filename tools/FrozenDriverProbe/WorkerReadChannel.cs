using System.Reflection;
using System.Threading.Channels;
using System.Threading.Tasks.Sources;

namespace Relayer.Diagnostics;

// Single-use diagnostic wrappers deliberately alter scheduling and retention. No production use.
internal sealed class WorkerReadChannel : Channel<byte>
{
    private static readonly FieldInfo SourceField = typeof(ValueTask<byte>).GetField("_obj", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new NotSupportedException("Expected native ValueTask<byte> source and token fields.");
    private static readonly FieldInfo TokenField = typeof(ValueTask<byte>).GetField("_token", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new NotSupportedException("Expected native ValueTask<byte> source and token fields.");
    private readonly Record?[] _records;
    private int _next;
    private int _finished;
    private int _reported;

    public WorkerReadChannel(Channel<byte> original, int capacity)
    {
        _records = new Record[capacity];
        Reader = new RecordingReader(this, original.Reader);
        Writer = original.Writer;
    }

    // Completed-channel notification precedes callback return. Establish a separate drain boundary.
    public void Drain(int expected)
    {
        if (!SpinWait.SpinUntil(() => Volatile.Read(ref _finished) == expected, TimeSpan.FromSeconds(30)))
            throw new TimeoutException("Original handler continuations did not drain.");
        if (Volatile.Read(ref _next) != expected) throw new InvalidOperationException("Unexpected native read count.");
    }

    public object Report(int skip)
    {
        Drain(_records.Length);
        if (Interlocked.Exchange(ref _reported, 1) != 0) throw new InvalidOperationException("Already reported.");
        var nativeSources = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var handlerSources = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var rows = new List<(long ReadBytes, long RegistrationBytes, long ContinuationBytes, bool FirstNative, bool FirstHandler)>();
        var types = new HashSet<string>();
        var readThreads = new HashSet<int>();
        var continuationThreads = new HashSet<int>();
        for (var i = 0; i < _records.Length; i++)
        {
            var record = _records[i]!;
            // Reservation order can differ from execution time across workers.
            var firstNative = nativeSources.Add(record.Source);
            var firstHandler = handlerSources.Add(record.HandlerSource!);
            types.Add(record.Source.GetType().FullName!);
            types.Add(record.HandlerSource!.GetType().FullName!);
            if (i < skip) continue;
            readThreads.Add(record.ReadThread);
            continuationThreads.Add(record.ContinuationThread);
            rows.Add((record.ReadBytes, record.RegistrationBytes, record.ContinuationBytes, firstNative, firstHandler));
        }
        return new
        {
            calls = rows.Count, nativeSourcesIncludingWarmup = nativeSources.Count,
            continuationTargetsIncludingWarmup = handlerSources.Count, sourceTypes = types.Order().ToArray(),
            readThreads = readThreads.Order().ToArray(), continuationThreads = continuationThreads.Order().ToArray(),
            histogram = rows.GroupBy(row => row).OrderBy(group => group.Key.ReadBytes).ThenBy(group => group.Key.RegistrationBytes).ThenBy(group => group.Key.ContinuationBytes)
                .ThenBy(group => group.Key.FirstNative).ThenBy(group => group.Key.FirstHandler)
                .Select(group => new { readBytes = group.Key.ReadBytes, registrationBytes = group.Key.RegistrationBytes, continuationBytes = group.Key.ContinuationBytes,
                    firstNativeInReadOrder = group.Key.FirstNative, firstContinuationTargetInReadOrder = group.Key.FirstHandler, calls = group.Count() }).ToArray()
        };
    }

    private sealed class RecordingReader(WorkerReadChannel owner, ChannelReader<byte> original) : ChannelReader<byte>
    {
        public override Task Completion => original.Completion;
        public override bool TryRead(out byte item) => original.TryRead(out item);
        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) => original.WaitToReadAsync(cancellationToken);
        public override ValueTask<byte> ReadAsync(CancellationToken cancellationToken = default)
        {
            var index = Interlocked.Increment(ref owner._next) - 1;
            if (index >= owner._records.Length) throw new InvalidOperationException("Worker read buffer full.");
            var before = GC.GetAllocatedBytesForCurrentThread();
            var pending = original.ReadAsync(cancellationToken);
            var bytes = GC.GetAllocatedBytesForCurrentThread() - before;
            if (pending.IsCompleted) throw new InvalidOperationException("Expected an original pending release read.");
            var source = SourceField.GetValue(pending) as IValueTaskSource<byte>
                ?? throw new NotSupportedException("Expected native IValueTaskSource<byte>.");
            var token = (short)TokenField.GetValue(pending)!;
            var record = new Record(owner, source, token, bytes, Environment.CurrentManagedThreadId);
            owner._records[index] = record;
            record.MarkReadReturn();
            return new ValueTask<byte>(record, 0);
        }
    }

    private sealed class Record(WorkerReadChannel owner, IValueTaskSource<byte> source, short token, long readBytes, int readThread) : IValueTaskSource<byte>
    {
        public object Source => source;
        public long ReadBytes => readBytes;
        public int ReadThread => readThread;
        public object? HandlerSource { get; private set; }
        public long RegistrationBytes { get; private set; }
        public long ContinuationBytes { get; private set; }
        private long _readReturnBytes;
        public void MarkReadReturn() => _readReturnBytes = GC.GetAllocatedBytesForCurrentThread();
        public int ContinuationThread { get; private set; }
        private Action<object?>? _continuation;
        private object? _state;
        public byte GetResult(short ignored) => source.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short ignored) => source.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short ignored, ValueTaskSourceOnCompletedFlags flags)
        {
            // No suspension between ReadAsync return and registration on the original handler thread.
            // Counter starts after all wrapper construction/reflection, before returning its ValueTask.
            RegistrationBytes = GC.GetAllocatedBytesForCurrentThread() - _readReturnBytes;
            if (Environment.CurrentManagedThreadId != readThread)
                throw new InvalidOperationException("Handler await registration changed threads.");
            _continuation = continuation;
            _state = state;
            // ValueTaskAwaiter passes the original state-machine MoveNextAction as state.
            HandlerSource = state is Delegate action ? action.Target : state;
            if (HandlerSource is null) throw new InvalidOperationException("Missing original continuation target.");
            source.OnCompleted(static value => ((Record)value!).Resume(), this, token, flags);
        }
        private void Resume()
        {
            ContinuationThread = Environment.CurrentManagedThreadId;
            var before = GC.GetAllocatedBytesForCurrentThread();
            try { _continuation!(_state); }
            finally
            {
                ContinuationBytes = GC.GetAllocatedBytesForCurrentThread() - before;
                Interlocked.Increment(ref owner._finished);
            }
        }
    }
}
