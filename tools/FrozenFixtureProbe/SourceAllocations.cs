using System.Reflection;
using System.Threading.Channels;

namespace Relayer.Diagnostics;

// Diagnostic reflection and strong-reference tracking are outside the byte counter.
// Retaining sources changes GC behavior: this is a separate instrumented control.
internal sealed class SourceAllocations(int capacity)
{
    private readonly HashSet<object> _sources = new(ReferenceEqualityComparer.Instance);
    private readonly (long Bytes, bool FirstSource)[] _records = new (long, bool)[capacity];
    private readonly FieldInfo _source = typeof(ValueTask).GetField("_obj", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new NotSupportedException("This runtime does not expose the expected ValueTask source field.");
    private readonly FieldInfo _readSource = typeof(ValueTask<byte>).GetField("_obj", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new NotSupportedException("Expected ValueTask<byte> source field.");
    private readonly HashSet<string> _types = [];
    private int _count;

    public bool Enabled { get; set; }

    public ValueTask Invoke<TMessage, TContext>(Func<TMessage, TContext, CancellationToken, ValueTask> invoke,
        TMessage message, TContext context, CancellationToken cancellationToken)
    {
        if (Enabled && _count == _records.Length) throw new InvalidOperationException("Source allocation buffer is full.");
        var before = GC.GetAllocatedBytesForCurrentThread();
        var pending = invoke(message, context, cancellationToken);
        var bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        // Never consume the ValueTask here. Reflection boxes it only after the counter stops.
        var source = _source.GetValue(pending) ?? throw new InvalidOperationException("Expected a pending source-backed handler.");
        if (pending.IsCompleted) throw new InvalidOperationException("Handler must remain suspended during source inspection.");
        _types.Add(source.GetType().FullName!);
        var first = _sources.Add(source);
        if (Enabled) _records[_count++] = (bytes, first);
        return pending;
    }

    public ValueTask<byte> Read(ChannelReader<byte> reader, CancellationToken cancellationToken)
    {
        if (Enabled && _count == _records.Length) throw new InvalidOperationException("Source allocation buffer is full.");
        var before = GC.GetAllocatedBytesForCurrentThread();
        var pending = reader.ReadAsync(cancellationToken);
        var bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        var source = _readSource.GetValue(pending) ?? throw new InvalidOperationException("Expected a pending native read source.");
        if (pending.IsCompleted) throw new InvalidOperationException("Read must remain suspended during source inspection.");
        _types.Add(source.GetType().FullName!);
        var first = _sources.Add(source);
        if (Enabled) _records[_count++] = (bytes, first);
        return pending;
    }

    public SourceAllocationReport Report()
    {
        if (Enabled) throw new InvalidOperationException("Stop source recording before reporting.");
        var groups = _records.Take(_count).GroupBy(static record => record)
            .OrderBy(static group => group.Key.FirstSource).ThenBy(static group => group.Key.Bytes)
            .Select(static group => new SourceAllocationBin(group.Key.Bytes, group.Key.FirstSource, group.Count())).ToArray();
        return new SourceAllocationReport(_count, _sources.Count, _types.Order().ToArray(), groups);
    }
}

internal sealed record SourceAllocationBin(long Bytes, bool FirstObservedSource, int Invocations);
internal sealed record SourceAllocationReport(int Invocations, int DistinctSourcesIncludingWarmup, IReadOnlyList<string> SourceTypes, IReadOnlyList<SourceAllocationBin> Histogram);
