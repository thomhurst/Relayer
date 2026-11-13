using Relayer.Transports;
namespace Relayer;

/// <summary>Configures a message route and its portable send settings.</summary>
public sealed class RouteBuilder<TMessage>
{
    private readonly string _destination;
    private Delegate? _orderingKey;
    private IOrderingKeySelector<TMessage>? _sendOrderingKey;
    private SendRouteOptions _sendOptions = new();
    private bool _built;

    internal RouteBuilder(string destination) => _destination = destination;

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
    public RouteBuilder<TMessage> OrderingKey(Func<TMessage, string> selector)
    {
        SetOrderingKey(selector);
        _sendOrderingKey = new StringOrderingKeySelector<TMessage>(selector);
        return this;
    }

    /// <summary>Keeps a scalar selector typed so transports can format it into a pooled buffer without boxing.</summary>
    public RouteBuilder<TMessage> OrderingKey<TKey>(Func<TMessage, TKey> selector)
        where TKey : IUtf8SpanFormattable
    {
        SetOrderingKey(selector);
        _sendOrderingKey = new FormattedOrderingKeySelector<TMessage, TKey>(selector);
        return this;
    }

    private RouteBuilder<TMessage> SetOrderingKey(Delegate selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (_built) throw new InvalidOperationException("The route declaration is frozen.");
        _orderingKey = selector;
        return this;
    }

    internal IRouteDefinition Build()
    {
        _built = true;
        return new RouteDefinition<TMessage>(_destination, _orderingKey, _sendOrderingKey, _sendOptions);
    }
}
