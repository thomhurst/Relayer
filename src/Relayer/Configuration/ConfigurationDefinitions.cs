using Relayer.Pipeline;

namespace Relayer.Transports;

/// <summary>The declared destinations and consumers, built once during registration.</summary>
public sealed class RelayerConfiguration
{
    internal RelayerConfiguration(TopologyMode topologyMode, TransportDefinition[] transports)
    {
        TopologyMode = topologyMode;
        Transports = Array.AsReadOnly(transports);
    }

    /// <summary>The topology mode captured during registration.</summary>
    public TopologyMode TopologyMode
    {
        get;
    }

    /// <summary>Transports in declaration order.</summary>
    public IReadOnlyList<TransportDefinition> Transports
    {
        get;
    }
}

/// <summary>A named transport and its typed route and consumer declarations.</summary>
public sealed class TransportDefinition
{
    private readonly Func<string, string>? _normalizeRouteDestination;

    internal TransportDefinition(string name, Type optionsType, IRouteDefinition[] routes,
        IConsumerDefinition[] consumers, TopologyMode? topologyMode,
        Action<IServiceProvider, List<string>> validateOptions, Func<string, string>? normalizeRouteDestination = null)
    {
        Name = name;
        OptionsType = optionsType;
        Routes = Array.AsReadOnly(routes);
        Consumers = Array.AsReadOnly(consumers);
        TopologyMode = topologyMode;
        _normalizeRouteDestination = normalizeRouteDestination;
        ValidateOptions = validateOptions;
    }

    /// <summary>The unique, case-sensitive transport and Options name.</summary>
    public string Name
    {
        get;
    }

    /// <summary>The native options type used by this transport.</summary>
    public Type OptionsType
    {
        get;
    }

    /// <summary>The transport override, or null to inherit the Relayer default.</summary>
    public TopologyMode? TopologyMode
    {
        get;
    }

    /// <summary>The declared destinations.</summary>
    public IReadOnlyList<IRouteDefinition> Routes
    {
        get;
    }

    /// <summary>The declared consumers.</summary>
    public IReadOnlyList<IConsumerDefinition> Consumers
    {
        get;
    }

    internal Action<IServiceProvider, List<string>> ValidateOptions
    {
        get;
    }

    internal (Type MessageType, string Transport, string Destination) GetRouteKey(IRouteDefinition route)
    {
        var destination = route.Destination;
        return (route.MessageType, Name, _normalizeRouteDestination?.Invoke(destination) ?? destination);
    }
}

/// <summary>A message route. CLR types identify registrations only, never wire messages.</summary>
public interface IRouteDefinition
{
    /// <summary>Portable send settings captured when the route declaration is frozen.</summary>
    SendRouteOptions SendOptions
    {
        get;
    }

    /// <summary>The CLR type used to select this declaration at startup.</summary>
    Type MessageType
    {
        get;
    }

    /// <summary>The native destination name.</summary>
    string Destination
    {
        get;
    }

    /// <summary>Whether this route requires native ordering by key, session, or message group.</summary>
    bool HasOrderingKey
    {
        get;
    }
}

/// <summary>A message route with a typed native ordering selector.</summary>
public sealed class RouteDefinition<TMessage> : IRouteDefinition, ISendRouteDefinition
{
    private readonly Delegate? _orderingKey;
    private readonly IOrderingKeySelector<TMessage>? _sendOrderingKey;
    private readonly DestinationGroup<TMessage>? _group;

    internal RouteDefinition(string destination, Delegate? orderingKey,
        IOrderingKeySelector<TMessage>? sendOrderingKey, SendRouteOptions sendOptions, DestinationGroup<TMessage>? group = null)
    {
        Destination = destination;
        _orderingKey = orderingKey;
        _sendOrderingKey = sendOrderingKey;
        SendOptions = sendOptions;
        _group = group;
    }

    /// <inheritdoc />
    public Type MessageType => typeof(TMessage);

    /// <inheritdoc />
    public string Destination
    {
        get;
    }

    /// <summary>Portable send settings captured during registration.</summary>
    public SendRouteOptions SendOptions
    {
        get;
    }

    internal Delegate? NativeConfigurator
    {
        get; init;
    }

    internal Type? NativeTransportOptionsType
    {
        get; init;
    }

    Delegate? ISendRouteDefinition.NativeConfigurator => NativeConfigurator;

    Type? ISendRouteDefinition.NativeTransportOptionsType => NativeTransportOptionsType;

    bool ISendRouteDefinition.IsAdditionalDestination => _group is not null && _group.Destinations[0] != Destination;

    bool ISendRouteDefinition.HasDestinationSelector => _group is not null;

    SendRouteRegistration? ISendRouteDefinition.CreateSendRegistration(string transportName)
    {
        return ((ISendRouteDefinition)this).IsAdditionalDestination ? null
                    : new SendRouteRegistration<TMessage>(transportName, _group?.Destinations ?? [Destination], _group?.Selector, SendOptions, _sendOrderingKey);
    }

    /// <summary>Whether a native ordering key selector was declared.</summary>
    public bool HasOrderingKey => _orderingKey is not null;

    /// <summary>Resolves the selector once at pipeline construction. Invoke the returned typed delegate directly.</summary>
    public Func<TMessage, TKey>? GetOrderingKeySelector<TKey>()
    {
        if (_orderingKey is null)
        {
            return null;
        }

        return _orderingKey as Func<TMessage, TKey>
            ?? throw new InvalidOperationException($"The ordering key for {typeof(TMessage)} is not {typeof(TKey)}.");
    }
}

/// <summary>The destinations of one route and the selector that chooses among them per message.</summary>
internal sealed class DestinationGroup<TMessage>(string[] destinations, Func<TMessage, string> selector)
{
    internal string[] Destinations
    {
        get;
    } = destinations;

    internal Func<TMessage, string> Selector
    {
        get;
    } = selector;
}

/// <summary>A consumer's portable configuration. Native consumer options remain typed on the concrete declaration.</summary>
public interface IConsumerDefinition
{
    /// <summary>The portable retry, redelivery and dead-letter policy.</summary>
    FailureOptions Failure => new();

    /// <summary>A consumer-specific classifier, or null to use the global registration or default.</summary>
    IFailureClassifier? FailureClassifier => null;

    /// <summary>The native source endpoint.</summary>
    string Source
    {
        get;
    }

    /// <summary>Explicit handler types for generated dispatch, in registration order.</summary>
    IReadOnlyList<Type> Handlers
    {
        get;
    }

    /// <summary>Whether this consumer selected <paramref name="descriptor"/>'s handler and message contract.</summary>
    bool Selects(HandlerDescriptor descriptor)
    {
        return Handlers.Contains(descriptor.HandlerType);
    }

    /// <summary>Startup-only pipeline configuration, in declaration order.</summary>
    IReadOnlyList<Action<ConsumePipelineBuilder>> PipelineConfiguration => Array.Empty<Action<ConsumePipelineBuilder>>();

    internal IReadOnlyList<ConsumerMessageAlias> MessageAliases => [];

    /// <summary>The maximum number of concurrent handler invocations.</summary>
    int Concurrency
    {
        get;
    }

    /// <summary>The maximum number of messages per batch; 1 disables batching.</summary>
    int BatchSize
    {
        get;
    }

    /// <summary>The maximum wait for a partial batch.</summary>
    TimeSpan BatchWait
    {
        get;
    }
}

/// <summary>A consumer with native transport-specific options.</summary>
public sealed class ConsumerDefinition<TConsumerOptions> : IConsumerDefinition where TConsumerOptions : class
{
    internal ConsumerDefinition(string source, TConsumerOptions options, Type[] handlers,
        int concurrency, int batchSize, TimeSpan batchWait, Action<ConsumePipelineBuilder>[]? pipelineConfiguration = null,
        FailureOptions? failure = null, IFailureClassifier? failureClassifier = null,
        HashSet<(Type Handler, Type Message, bool IsBatch)>? selected = null, ConsumerMessageAlias[]? messageAliases = null)
    {
        _selected = selected;
        HasFailurePolicy = failure is not null;
        Failure = failure ?? new();
        FailureClassifier = failureClassifier;
        Source = source;
        PipelineConfiguration = Array.AsReadOnly(pipelineConfiguration ?? []);
        _messageAliases = Array.AsReadOnly(messageAliases ?? []);
        Options = options;
        Handlers = Array.AsReadOnly(handlers);
        Concurrency = concurrency;
        BatchSize = batchSize;
        BatchWait = batchWait;
    }

    /// <inheritdoc />
    public string Source
    {
        get;
    }
    private readonly HashSet<(Type Handler, Type Message, bool IsBatch)>? _selected;
    private readonly IReadOnlyList<ConsumerMessageAlias> _messageAliases;

    /// <inheritdoc />
    public IReadOnlyList<Type> Handlers
    {
        get;
    }

    /// <inheritdoc />
    public bool Selects(HandlerDescriptor descriptor)
    {
        return _selected is null
                ? Handlers.Contains(descriptor.HandlerType)
                : _selected.Contains((descriptor.HandlerType, descriptor.MessageType, descriptor.IsBatch));
    }

    /// <inheritdoc />
    public int Concurrency
    {
        get;
    }

    /// <inheritdoc />
    public int BatchSize
    {
        get;
    }

    /// <inheritdoc />
    public TimeSpan BatchWait
    {
        get;
    }

    /// <inheritdoc />
    public IReadOnlyList<Action<ConsumePipelineBuilder>> PipelineConfiguration
    {
        get;
    }

    IReadOnlyList<ConsumerMessageAlias> IConsumerDefinition.MessageAliases => _messageAliases;

    /// <inheritdoc />
    public FailureOptions Failure
    {
        get;
    }

    internal bool HasFailurePolicy
    {
        get;
    }

    /// <inheritdoc />
    public IFailureClassifier? FailureClassifier
    {
        get;
    }

    /// <summary>The transport-specific consumer options, unchanged.</summary>
    public TConsumerOptions Options
    {
        get;
    }
}
