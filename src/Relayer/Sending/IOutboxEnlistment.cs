namespace Relayer;

/// <summary>Implemented by an outbox integration and registered as a scoped service.</summary>
public interface IOutboxEnlistment
{
    /// <summary>Whether this scope owns an active outbox transaction or a selected pending unit of work.</summary>
    bool IsActive { get; }

    /// <summary>Whether registered dispatch workers deliver future-due unordered rows from this outbox.</summary>
    bool SupportsScheduling => false;

    /// <summary>Whether registered native-order dispatch workers deliver future-due ordered rows from this outbox.</summary>
    bool SupportsOrderedScheduling => false;

    /// <summary>Takes an owned copy of the original row for the active transaction before completing.</summary>
    /// <remarks>
    /// Observe cancellation. Payload storage is borrowed only until completion. The transaction owner persists
    /// buffered rows before commit. Id, MessageType and CreatedAt come from the original send envelope, including raw mode.
    /// </remarks>
    ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken);
}
