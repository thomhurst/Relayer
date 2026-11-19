namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Selects a DbContext for outbox sends. Dispose nested selections in reverse creation order.</summary>
public interface IOutboxSelection : IDisposable
{
    /// <summary>Enlists a routed message with the selected context. Its transaction commit makes the message durable.</summary>
    ValueTask EnqueueAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default);

    /// <summary>Enlists a routed message with per-send options. Its transaction commit makes the message durable.</summary>
    ValueTask EnqueueAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default);
}
