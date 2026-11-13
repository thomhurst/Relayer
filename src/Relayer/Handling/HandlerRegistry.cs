using System.Collections.ObjectModel;

namespace Relayer;

/// <summary>Explicit generated descriptors. Lookups belong to pipeline composition, not message dispatch.</summary>
public sealed class HandlerRegistry
{
    private readonly Dictionary<(Type Handler, Type Message, bool IsBatch), HandlerDescriptor> _descriptors = new();
    private readonly List<HandlerDescriptor> _items = [];
    private readonly ReadOnlyCollection<HandlerDescriptor> _view;
    private bool _isFrozen;

    /// <summary>Creates an empty registry.</summary>
    public HandlerRegistry() => _view = _items.AsReadOnly();

    /// <summary>The registered single-message and batch descriptors, in registration order.</summary>
    public IReadOnlyList<HandlerDescriptor> Descriptors => _view;

    /// <summary>Adds a generated descriptor. Repeated registration is idempotent.</summary>
    public void Add(HandlerDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        if (_isFrozen)
        {
            throw new InvalidOperationException("Handlers cannot be registered after AddRelayer configuration completes.");
        }

        if (_descriptors.TryAdd((descriptor.HandlerType, descriptor.MessageType, descriptor.IsBatch), descriptor))
        {
            _items.Add(descriptor);
        }
    }

    /// <summary>Selects a single-message dispatcher at pipeline composition time.</summary>
    public HandlerDescriptor<TMessage> GetHandler<TMessage, THandler>() where THandler : IHandler<TMessage>
        => (HandlerDescriptor<TMessage>)Get(typeof(THandler), typeof(TMessage), false);

    /// <summary>Selects a batch dispatcher at pipeline composition time.</summary>
    public BatchHandlerDescriptor<TMessage> GetBatchHandler<TMessage, THandler>() where THandler : IBatchHandler<TMessage>
        => (BatchHandlerDescriptor<TMessage>)Get(typeof(THandler), typeof(TMessage), true);

    internal void Freeze() => _isFrozen = true;

    private HandlerDescriptor Get(Type handler, Type message, bool isBatch)
        => _descriptors.TryGetValue((handler, message, isBatch), out var descriptor)
            ? descriptor
            : throw new InvalidOperationException(
                $"Handler '{handler}' for message '{message}' (batch: {isBatch}) is not registered. " +
                "Call consumer.Handler<THandler>() inside its consumer configuration callback.");
}
