namespace Relayer;

/// <summary>Identifies a single-message or batch handler without runtime interface discovery.</summary>
public interface IHandlerMarker;

/// <summary>Handles messages of type <typeparamref name="TMessage"/>. Resolved from a fresh DI scope per message.</summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public interface IHandler<in TMessage> : IHandlerMarker
{
    /// <summary>Handles a single message.</summary>
    ValueTask HandleAsync(TMessage message, MessageContext context, CancellationToken cancellationToken);
}

/// <summary>Handles batches of <typeparamref name="TMessage"/>. Each message can succeed or fail independently.</summary>
/// <typeparam name="TMessage">The message type.</typeparam>
public interface IBatchHandler<TMessage> : IHandlerMarker
{
    /// <summary>Handles a batch and reports any per-message failures.</summary>
    ValueTask<BatchResult> HandleAsync(Batch<TMessage> batch, CancellationToken cancellationToken);
}
