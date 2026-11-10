namespace Relayer.Diagnostics;

// A separate instrumented control. Never span an await with a thread-local counter.
internal sealed class InvocationAllocations(int capacity)
{
    private readonly long[] _bytes = new long[capacity];
    private int _count;

    public bool Enabled { get; set; }

    public ValueTask Invoke<TMessage, TContext>(
        Func<TMessage, TContext, CancellationToken, ValueTask> invoke,
        TMessage message, TContext context, CancellationToken cancellationToken)
    {
        if (!Enabled) return invoke(message, context, cancellationToken);
        if (_count == _bytes.Length) throw new InvalidOperationException("Invocation allocation buffer is full.");
        var before = GC.GetAllocatedBytesForCurrentThread();
        var pending = invoke(message, context, cancellationToken);
        var bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        // Buffer writes and all result processing are outside the counted invocation.
        _bytes[_count++] = bytes;
        return pending;
    }

    public ValueTask<TResult> Invoke<TResult>(Func<ValueTask<TResult>> invoke)
    {
        if (!Enabled) return invoke();
        if (_count == _bytes.Length) throw new InvalidOperationException("Invocation allocation buffer is full.");
        var before = GC.GetAllocatedBytesForCurrentThread();
        var pending = invoke();
        var bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        _bytes[_count++] = bytes;
        return pending;
    }

    public InvocationAllocationReport Report()
    {
        if (Enabled) throw new InvalidOperationException("Stop recording before processing allocation results.");
        Array.Sort(_bytes, 0, _count);
        var histogram = new List<InvocationAllocationBin>();
        long total = 0;
        for (var i = 0; i < _count;)
        {
            var bytes = _bytes[i];
            var start = i;
            do { total = checked(total + _bytes[i++]); } while (i < _count && _bytes[i] == bytes);
            histogram.Add(new InvocationAllocationBin(bytes, i - start));
        }
        return new InvocationAllocationReport(_count, total, histogram);
    }
}

internal sealed record InvocationAllocationBin(long Bytes, int Invocations);
internal sealed record InvocationAllocationReport(int Invocations, long Bytes, IReadOnlyList<InvocationAllocationBin> Histogram);
