using System.Diagnostics;

namespace Relayer.Transports;

/// <summary>Values supplied for one operation. Borrowed links need remain valid only until Start returns.</summary>
public readonly record struct MessageTraceData
{
    /// <summary>The message's immutable creation context, if already present.</summary>
    public ActivityContext CreationContext { get; init; }
    /// <summary>Creation contexts for a batch operation.</summary>
    public ReadOnlyMemory<ActivityLink> Links { get; init; }
    /// <summary>Set only for a batch operation, including a batch containing one message.</summary>
    public int? BatchCount { get; init; }
    /// <summary>The wire message ID.</summary>
    public string? MessageId { get; init; }
    /// <summary>The stable CloudEvents message type, never the CLR type name.</summary>
    public string? EventType { get; init; }
    /// <summary>The CloudEvents source URI reference.</summary>
    public string? EventSource { get; init; }
    /// <summary>The correlation ID.</summary>
    public string? ConversationId { get; init; }
    /// <summary>The native partition identifier, supplied to samplers at span start.</summary>
    public string? PartitionId { get; init; }
    /// <summary>The serialized body size. Recorded only when explicitly enabled.</summary>
    public long? BodySize { get; init; }
}

/// <summary>The operation types emitted by Relayer. Internal polling never creates receive spans.</summary>
public enum MessagingOperation
{
    /// <summary>One message's creation.</summary>
    Create,
    /// <summary>Sending one or more messages.</summary>
    Send,
    /// <summary>Processing one or more messages.</summary>
    Process,
    /// <summary>A native settlement call.</summary>
    Settle,
}
