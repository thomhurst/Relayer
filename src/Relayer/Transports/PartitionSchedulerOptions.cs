namespace Relayer.Transports;

/// <summary>Fixed scheduler limits. Each assignment allocates reusable storage within these limits.</summary>
public sealed class PartitionSchedulerOptions
{
    /// <summary>Maximum retained deliveries per assignment, including completed deliveries behind a gap.</summary>
    public int MaxBufferedPerPartition { get; init; } = 64;
    /// <summary>Maximum retained deliveries across assignments, including late work from lost ownership.</summary>
    public int MaxBufferedDeliveries { get; init; } = 1024;
    /// <summary>Maximum simultaneous handlers across all assignments, including revoked or lost work.</summary>
    public int MaxConcurrency { get; init; } = 64;
    /// <summary>Maximum simultaneous handlers in one assignment.</summary>
    public int ConcurrencyPerPartition { get; init; } = 1;
}
