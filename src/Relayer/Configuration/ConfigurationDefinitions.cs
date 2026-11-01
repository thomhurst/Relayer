namespace Relayer;

/// <summary>The declared destinations and consumers, built once during registration.</summary>
public sealed class RelayerConfiguration
{
    internal RelayerConfiguration(TopologyMode topologyMode, TransportDefinition[] transports)
    {
        TopologyMode = topologyMode;
        Transports = Array.AsReadOnly(transports);
    }

    /// <summary>The topology mode captured during registration.</summary>
    public TopologyMode TopologyMode { get; }
    /// <summary>Transports in declaration order.</summary>
    public IReadOnlyList<TransportDefinition> Transports { get; }
}

/// <summary>A named transport and its typed route and consumer declarations.</summary>
public sealed class TransportDefinition
{
    internal TransportDefinition(string name, Type optionsType, IRouteDefinition[] routes,
        IConsumerDefinition[] consumers, TopologyMode? topologyMode, Action<IServiceProvider, List<string>> validateOptions)
    {
        Name = name;
        OptionsType = optionsType;
        Routes = Array.AsReadOnly(routes);
        Consumers = Array.AsReadOnly(consumers);
        TopologyMode = topologyMode;
        ValidateOptions = validateOptions;
    }

    /// <summary>The unique, case-sensitive transport and Options name.</summary>
    public string Name { get; }
    /// <summary>The native options type used by this transport.</summary>
    public Type OptionsType { get; }
    /// <summary>The transport override, or null to inherit the Relayer default.</summary>
    public TopologyMode? TopologyMode { get; }
    /// <summary>The declared destinations.</summary>
    public IReadOnlyList<IRouteDefinition> Routes { get; }
    /// <summary>The declared consumers.</summary>
    public IReadOnlyList<IConsumerDefinition> Consumers { get; }
    internal Action<IServiceProvider, List<string>> ValidateOptions { get; }
}

/// <summary>A message route. CLR types identify registrations only, never wire messages.</summary>
public interface IRouteDefinition
{
    /// <summary>The CLR type used to select this declaration at startup.</summary>
    public Type MessageType { get; }
    /// <summary>The native destination name.</summary>
    public string Destination { get; }
    /// <summary>Whether this route requires native ordering by key, session, or message group.</summary>
    public bool HasOrderingKey { get; }
}

/// <summary>A route with unchanged transport-specific options and a typed native ordering selector.</summary>
public sealed class RouteDefinition<TMessage, TRouteOptions> : IRouteDefinition, ISendRouteDefinition where TRouteOptions : class
{
    private readonly Delegate? _orderingKey;
    private readonly IOrderingKeySelector<TMessage>? _sendOrderingKey;

    internal RouteDefinition(string destination, TRouteOptions options, Delegate? orderingKey,
        IOrderingKeySelector<TMessage>? sendOrderingKey, SendRouteOptions sendOptions)
    {
        Destination = destination;
        Options = options;
        _orderingKey = orderingKey;
        _sendOrderingKey = sendOrderingKey;
        SendOptions = sendOptions;
    }

    /// <inheritdoc />
    public Type MessageType => typeof(TMessage);
    /// <inheritdoc />
    public string Destination { get; }

    /// <summary>The transport-specific route options, unchanged.</summary>
    public TRouteOptions Options { get; }
    /// <summary>Portable send settings captured during registration.</summary>
    public SendRouteOptions SendOptions { get; }

    SendRouteRegistration ISendRouteDefinition.CreateSendRegistration(string transportName) =>
        new SendRouteRegistration<TMessage>(transportName, Destination, SendOptions, _sendOrderingKey);
    /// <summary>Whether a native ordering key selector was declared.</summary>
    public bool HasOrderingKey => _orderingKey is not null;

    /// <summary>Resolves the selector once at pipeline construction. Invoke the returned typed delegate directly.</summary>
    public Func<TMessage, TKey>? GetOrderingKeySelector<TKey>()
    {
        if (_orderingKey is null) return null;
        return _orderingKey as Func<TMessage, TKey>
            ?? throw new InvalidOperationException($"The ordering key for {typeof(TMessage)} is not {typeof(TKey)}.");
    }
}

/// <summary>A consumer's portable configuration. Native consumer options remain typed on the concrete declaration.</summary>
public interface IConsumerDefinition
{
    /// <summary>The native source endpoint.</summary>
    public string Source { get; }
    /// <summary>Explicit handler types for generated dispatch, in registration order.</summary>
    public IReadOnlyList<Type> Handlers { get; }
    /// <summary>The maximum number of concurrent handler invocations.</summary>
    public int Concurrency { get; }
    /// <summary>The maximum number of messages per batch; 1 disables batching.</summary>
    public int BatchSize { get; }
    /// <summary>The maximum wait for a partial batch.</summary>
    public TimeSpan BatchWait { get; }
}

/// <summary>A consumer with native transport-specific options.</summary>
public sealed class ConsumerDefinition<TConsumerOptions> : IConsumerDefinition where TConsumerOptions : class
{
    internal ConsumerDefinition(string source, TConsumerOptions options, Type[] handlers,
        int concurrency, int batchSize, TimeSpan batchWait)
    {
        Source = source;
        Options = options;
        Handlers = Array.AsReadOnly(handlers);
        Concurrency = concurrency;
        BatchSize = batchSize;
        BatchWait = batchWait;
    }

    /// <inheritdoc />
    public string Source { get; }
    /// <inheritdoc />
    public IReadOnlyList<Type> Handlers { get; }
    /// <inheritdoc />
    public int Concurrency { get; }
    /// <inheritdoc />
    public int BatchSize { get; }
    /// <inheritdoc />
    public TimeSpan BatchWait { get; }

    /// <summary>The transport-specific consumer options, unchanged.</summary>
    public TConsumerOptions Options { get; }
}
