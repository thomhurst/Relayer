namespace Relayer;

/// <summary>Implemented by an outbox integration and registered as a scoped service.</summary>
public interface IOutboxEnlistment
{
    /// <summary>Whether this scope currently has an active outbox transaction.</summary>
    bool IsActive { get; }

    /// <summary>Copies the message into the active transaction before completing.</summary>
    /// <remarks>Observe cancellation. The message's storage is borrowed only until completion.</remarks>
    ValueTask EnlistAsync(string transportName, OutgoingMessage message, CancellationToken cancellationToken);
}
