namespace Relayer;

/// <summary>Configures a route while preserving the transport's option type.</summary>
public sealed class RouteBuilder<TMessage, TRouteOptions> where TRouteOptions : class, new()
{
    private readonly string _destination;
    private Delegate? _orderingKey;
    private IOrderingKeySelector<TMessage>? _sendOrderingKey;
    private SendRouteOptions _sendOptions = new();
    private bool _built;

    internal RouteBuilder(string destination) => _destination = destination;

    /// <summary>The transport's native route options.</summary>
    public TRouteOptions Options { get; } = new();

    /// <summary>Portable send settings captured when the route declaration is frozen.</summary>
    public SendRouteOptions SendOptions
    {
        get => _sendOptions;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            if (_built) throw new InvalidOperationException("The route declaration is frozen.");
            _sendOptions = value;
        }
    }

    /// <summary>Derives a native partition key, session ID or message group ID from a string.</summary>
    public RouteBuilder<TMessage, TRouteOptions> OrderingKey(Func<TMessage, string> selector)
    {
        SetOrderingKey(selector);
        _sendOrderingKey = new StringOrderingKeySelector<TMessage>(selector);
        return this;
    }

    /// <summary>Keeps a scalar selector typed so transports can format it into a pooled buffer without boxing.</summary>
    public RouteBuilder<TMessage, TRouteOptions> OrderingKey<TKey>(Func<TMessage, TKey> selector)
        where TKey : IUtf8SpanFormattable
    {
        SetOrderingKey(selector);
        _sendOrderingKey = new FormattedOrderingKeySelector<TMessage, TKey>(selector);
        return this;
    }

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
        return new RouteDefinition<TMessage, TRouteOptions>(_destination, Options, _orderingKey, _sendOrderingKey, _sendOptions);
    }
}
