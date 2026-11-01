using System.Buffers;
using Relayer.Serialization;

namespace Relayer.Testing;

/// <summary>An owned snapshot of a message observed by the harness.</summary>
public sealed class HarnessMessage
{
    internal HarnessMessage(string? messageId, string? wireType, ReadOnlyMemory<byte> body,
        string? transportName = null, string? destination = null, int attempt = 1,
        object? message = null, bool usesOutbox = false)
    {
        MessageId = messageId;
        WireType = wireType;
        Body = body;
        TransportName = transportName;
        Destination = destination;
        Attempt = attempt;
        Message = message;
        UsesOutbox = usesOutbox;
    }

    /// <summary>The stable ID, or null for a malformed or raw envelope without an ID.</summary>
    public string? MessageId { get; }
    /// <summary>The stable wire type, or null when the envelope has no type.</summary>
    public string? WireType { get; }
    /// <summary>A copy of the serialized body. Logical sends retain Message instead.</summary>
    public ReadOnlyMemory<byte> Body { get; }
    /// <summary>The named transport, when correlated with a send in this host.</summary>
    public string? TransportName { get; }
    /// <summary>The declared destination, when correlated with a send in this host.</summary>
    public string? Destination { get; }
    /// <summary>The delivery attempt, starting at one.</summary>
    public int Attempt { get; }
    /// <summary>The original logical send object. Treat this reference as read-only.</summary>
    public object? Message { get; }
    /// <summary>Whether the logical send uses an active outbox transaction.</summary>
    public bool UsesOutbox { get; }

    /// <summary>Reads a logical object or deserializes an owned receive/dispatch snapshot.</summary>
    public TMessage Read<TMessage>(IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return Message is TMessage message ? message : serializer.Deserialize<TMessage>(new ReadOnlySequence<byte>(Body));
    }
}

/// <summary>A failed operation, including the original exception.</summary>
public sealed record HarnessFailure(HarnessMessage Message, Exception Error, HarnessOperation Operation);

/// <summary>The boundary at which an operation failed.</summary>
public enum HarnessOperation
{
    /// <summary>A logical send, including middleware, serialization or enlistment.</summary>
    Send,
    /// <summary>A native transport send or batch dispatch.</summary>
    Dispatch,
    /// <summary>A consume attempt or its durable failure handling.</summary>
    Consume,
}

/// <summary>A successful native dead-letter operation and its owned error metadata.</summary>
public sealed record HarnessDeadLetter(HarnessMessage Message, DeadLetterInfo Error);
