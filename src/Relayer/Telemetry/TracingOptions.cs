namespace Relayer;

/// <summary>Tracing policy, also bindable through the Options pattern.</summary>
public sealed class TracingOptions
{
    /// <summary>Use a single message's creation context as parent when no activity is current.</summary>
    public bool CreationContextAsParent { get; init; } = true;

    /// <summary>Create a producer span for each message before a batch send.</summary>
    public bool CreateSpansForBatches { get; init; } = true;

    /// <summary>Record serialized body size on single-message spans.</summary>
    public bool IncludeBodySize { get; init; }
}
