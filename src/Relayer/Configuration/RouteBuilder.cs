namespace Relayer;

/// <summary>Configures a route while preserving the transport's option type.</summary>
public sealed class RouteBuilder<TMessage, TRouteOptions> where TRouteOptions : class, new()
{
    private readonly string _destination;
    private Delegate? _orderingKey;
    private bool _built;

    internal RouteBuilder(string destination) => _destination = destination;

    /// <summary>The transport's native route options.</summary>
    public TRouteOptions Options { get; } = new();

    /// <summary>Derives a native partition key, session ID or message group ID from a string.</summary>
    public RouteBuilder<TMessage, TRouteOptions> OrderingKey(Func<TMessage, string> selector) => SetOrderingKey(selector);

    /// <summary>Keeps a scalar selector typed so transports can format it into a pooled buffer without boxing.</summary>
    public RouteBuilder<TMessage, TRouteOptions> OrderingKey<TKey>(Func<TMessage, TKey> selector)
        where TKey : IUtf8SpanFormattable => SetOrderingKey(selector);

    private RouteBuilder<TMessage, TRouteOptions> SetOrderingKey(Delegate selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (_built) throw new InvalidOperationException("The route declaration is frozen.");
        _orderingKey = selector;
        return this;
    }

    internal IRouteDefinition Build()
    {
        _built = true;
        return new RouteDefinition<TMessage, TRouteOptions>(_destination, Options, _orderingKey);
    }
}
