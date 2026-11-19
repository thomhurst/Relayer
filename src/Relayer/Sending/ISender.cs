namespace Relayer;

/// <summary>
/// Sends messages to their configured routes. When an outbox is registered, SendAsync requires an active
/// outbox transaction or selected pending unit of work in the current scope and enlists automatically.
/// Use SendWithoutOutboxAsync for intentional direct sends.
/// </summary>
public interface ISender
{
    /// <summary>Sends a message using its configured route.</summary>
    /// <exception cref="InvalidOperationException">An outbox is registered but no outbox transaction or pending unit of work is active.</exception>
    /// <remarks>In a handler, sends outside the outbox are buffered until the handler, middleware and scope disposal succeed. A failed attempt discards them.</remarks>
    ValueTask SendAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default);

    /// <summary>Sends a message, customising headers, scheduling or the native outgoing message.</summary>
    /// <exception cref="InvalidOperationException">An outbox is registered but no outbox transaction or pending unit of work is active.</exception>
    /// <remarks>In a handler, sends outside the outbox are buffered until the handler, middleware and scope disposal succeed. A failed attempt discards them.</remarks>
    ValueTask SendAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default);

    /// <summary>Sends a message without enlisting it in the outbox.</summary>
    /// <remarks>In a handler, sends outside the outbox are buffered until the handler, middleware and scope disposal succeed. A failed attempt discards them.</remarks>
    ValueTask SendWithoutOutboxAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default);

    /// <summary>Sends a message with per-send configuration without enlisting it in the outbox.</summary>
    /// <remarks>In a handler, sends outside the outbox are buffered until the handler, middleware and scope disposal succeed. A failed attempt discards them.</remarks>
    ValueTask SendWithoutOutboxAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default);

    /// <summary>Requires an active outbox transaction and enlists the send.</summary>
    /// <remarks>In a handler, the send enlists in the active transaction, which rolls back if the attempt fails.</remarks>
    ValueTask SendViaOutboxAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default);

    /// <summary>Enlists a configured send, requiring an active outbox transaction.</summary>
    /// <remarks>In a handler, the send enlists in the active transaction, which rolls back if the attempt fails.</remarks>
    ValueTask SendViaOutboxAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default);
}
