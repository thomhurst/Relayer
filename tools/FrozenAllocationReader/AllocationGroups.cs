namespace Relayer.Diagnostics;

// Diagnostic aggregation only; thread IDs are local to a single trace/process.
internal sealed class AllocationGroups
{
    private readonly Dictionary<(int Thread, string Type, string Stack), AllocationGroup> _groups = new();

    public IEnumerable<AllocationGroup> Values => _groups.Values;

    public void Add(int threadId, string type, string stack, long sampledBytes)
    {
        var key = (threadId, type, stack);
        if (!_groups.TryGetValue(key, out var group))
            _groups.Add(key, group = new AllocationGroup(threadId, type, stack));
        group.Samples++;
        group.SampledBytes += sampledBytes;
    }
}

internal sealed class AllocationGroup(int threadId, string type, string stack)
{
    public int ThreadId { get; } = threadId;
    public string Type { get; } = type;
    public string Stack { get; } = stack;
    public long Samples { get; set; }
    public long SampledBytes { get; set; }
}
