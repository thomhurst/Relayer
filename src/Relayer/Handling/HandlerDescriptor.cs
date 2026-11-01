namespace Relayer;

/// <summary>Generated handler metadata, selected once when composing a consumer.</summary>
/// <param name="handlerType">The concrete handler type. Used only during configuration.</param>
/// <param name="messageType">The handled CLR type. Used only during configuration.</param>
/// <param name="isBatch">Whether this descriptor dispatches batches.</param>
public abstract class HandlerDescriptor(Type handlerType, Type messageType, bool isBatch)
{
    /// <summary>The concrete handler type.</summary>
    public Type HandlerType { get; } = handlerType;

    /// <summary>The handled CLR type, never the wire identity.</summary>
    public Type MessageType { get; } = messageType;

    /// <summary>Whether this descriptor dispatches batches.</summary>
    public bool IsBatch { get; } = isBatch;

    /// <summary>Resolves the handler from the message's DI scope before dispatch.</summary>
    public abstract IHandlerMarker Resolve(IServiceProvider services);
}

/// <summary>A generated, typed single-message dispatcher. Cache it when composing the pipeline.</summary>
public abstract class HandlerDescriptor<TMessage>(Type handlerType)
    : HandlerDescriptor(handlerType, typeof(TMessage), false)
{
    /// <summary>Invokes the resolved handler without reflection, lookup, or intermediate tasks.</summary>
    public abstract ValueTask DispatchAsync(
        IHandlerMarker handler, TMessage message, MessageContext context, CancellationToken cancellationToken);
}

/// <summary>A generated, typed batch dispatcher. Cache it when composing the pipeline.</summary>
public abstract class BatchHandlerDescriptor<TMessage>(Type handlerType)
    : HandlerDescriptor(handlerType, typeof(TMessage), true)
{
    /// <summary>Invokes the resolved handler, preserving its per-message results.</summary>
    public abstract ValueTask<BatchResult> DispatchAsync(
        IHandlerMarker handler, Batch<TMessage> batch, CancellationToken cancellationToken);
}
