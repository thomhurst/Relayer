namespace Relayer;

/// <summary>
/// Sends messages to their configured routes. When an outbox is configured and a transaction is active
/// in the current scope, sends are enlisted in the outbox automatically.
/// </summary>
public interface ISender
{
    /// <summary>Sends a message using its configured route.</summary>
    ValueTask SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default);

    /// <summary>Sends a message, customising headers, scheduling or the native outgoing message.</summary>
    ValueTask SendAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default);
}
