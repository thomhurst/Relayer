namespace Relayer;

/// <summary>Configures bounded request state and the ordinary send route and reply consumer.</summary>
public sealed class RequestBuilder<TRequest, TConsumerOptions>
    where TConsumerOptions : class, new()
{
    private RequestOptions _options = new();
    internal Action<RouteBuilder<TRequest>>? ConfigureSend { get; private set; }
    internal Action<ConsumerBuilder<TConsumerOptions>>? ConfigureReplies { get; private set; }

    internal RequestBuilder() { }

    /// <summary>Bounds pending requests and selects their deadline clock. Each call still supplies its timeout.</summary>
    public RequestOptions Options
    {
        get => _options;
        set { ArgumentNullException.ThrowIfNull(value); _options = value; }
    }

    /// <summary>Configures the ordinary request send route and its portable send settings.</summary>
    public RequestBuilder<TRequest, TConsumerOptions> Send(Action<RouteBuilder<TRequest>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        ConfigureSend += configure;
        return this;
    }

    /// <summary>Configures the dedicated reply consumer, preserving native consumer options.</summary>
    public RequestBuilder<TRequest, TConsumerOptions> Replies(Action<ConsumerBuilder<TConsumerOptions>> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        ConfigureReplies += configure;
        return this;
    }
}
