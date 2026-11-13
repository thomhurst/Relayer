using Relayer.Transports;
using Relayer.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Serialization;

namespace Relayer;

/// <summary>The common declaration surface used by transport-specific extension methods.</summary>
public sealed class TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions>
    where TTransportOptions : class, new()
    where TRouteOptions : class, new()
    where TConsumerOptions : class, new()
{
    private readonly IServiceCollection _services;
    private readonly HandlerRegistry _handlers;
    private readonly List<IRouteDefinition> _routes = [];
    private readonly List<IConsumerDefinition> _consumers = [];
    private bool _built;
    private TopologyMode? _topologyMode;
    private SendRouteOptions _sendDefaults = new();
    private bool _hasSerializer;

    internal TransportBuilder(IServiceCollection services, HandlerRegistry handlers, string name)
    {
        _services = services;
        _handlers = handlers;
        Name = name;
        Options = services.AddOptions<TTransportOptions>(name);
    }

    /// <summary>The transport's case-sensitive registration and Options name.</summary>
    public string Name { get; }

    /// <summary>Overrides the Relayer topology mode for this transport; null inherits the default.</summary>
    public TopologyMode? TopologyMode
    {
        get => _topologyMode;
        set { EnsureMutable(); _topologyMode = value; }
    }

    /// <summary>Named Options pattern configuration, binding, validation and reload registration.</summary>
    public OptionsBuilder<TTransportOptions> Options { get; }

    /// <summary>Default portable send settings for routes declared afterward. A route may replace them with SendOptions.</summary>
    public SendRouteOptions SendDefaults
    {
        get => _sendDefaults;
        set { EnsureMutable(); ArgumentNullException.ThrowIfNull(value); _sendDefaults = value; }
    }

    /// <summary>Declares a message destination. Whether this sends or publishes is a native routing concern.</summary>
    public TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions> Send<TMessage>(string destination,
        Action<RouteBuilder<TMessage, TRouteOptions>>? configure = null)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(destination);
        var route = new RouteBuilder<TMessage, TRouteOptions>(destination);
        route.SendOptions = _sendDefaults;
        configure?.Invoke(route);
        _routes.Add(route.Build());
        return this;
    }

    /// <summary>Declares a request send route and a dedicated, process-local reply consumer.</summary>
    /// <remarks>Give each requesting process its own reply endpoint. The responder declares an ordinary Send route to that endpoint.</remarks>
    public TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions> Request<TRequest, TResponse>(
        string destination, string replyAddress,
        Action<RequestBuilder<TRequest, TRouteOptions, TConsumerOptions>>? configure = null)
    {
        EnsureMutable();
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentException.ThrowIfNullOrWhiteSpace(replyAddress);
        var request = new RequestBuilder<TRequest, TRouteOptions, TConsumerOptions>();
        configure?.Invoke(request);
        var options = request.Options;
        options.Validate();
        if (_services.Any(service => service.ServiceType == typeof(IRequestClient<TRequest, TResponse>)))
            throw new InvalidOperationException("This request/response pair already has a registration. Declare it once.");
        _services.AddSingleton(new RequestReplies<TRequest, TResponse>(Name, replyAddress, options));
        _services.AddScoped<IRequestClient<TRequest, TResponse>, RequestClient<TRequest, TResponse>>();
        _services.AddSingleton<IRequestRegistration, RequestRegistration<TRequest, TResponse>>();
        Send(destination, request.ConfigureSend);
        Consume(replyAddress, consumer =>
        {
            request.ConfigureReplies?.Invoke(consumer);
            ((IConsumerBuilder)consumer).AddHandler(new ReplyDescriptor<TRequest, TResponse>());
        });
        return this;
    }

    /// <summary>Declares a consumer on a native source endpoint.</summary>
    public TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions> Consume(string source,
        Action<ConsumerBuilder<TConsumerOptions>> configure)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(configure);
        var consumer = new ConsumerBuilder<TConsumerOptions>(_services, _handlers, source);
        configure(consumer);
        _consumers.Add(consumer.Build());
        return this;
    }

    private void EnsureMutable()
    {
        if (_built) throw new InvalidOperationException("The transport declaration is frozen.");
    }

    internal void RegisterSerializer(Func<IServiceProvider, IMessageSerializer> factory)
    {
        EnsureMutable();
        if (_hasSerializer) throw new InvalidOperationException($"Transport '{Name}' already has a serializer. Call UseSerializer once.");
        _hasSerializer = true;
        _services.AddKeyedSingleton<IMessageSerializer>(Name, (provider, _) => factory(provider)
            ?? throw new InvalidOperationException($"Transport '{Name}': the serializer factory returned null."));
    }

    internal TransportDefinition Build()
    {
        _built = true;
        return new TransportDefinition(Name, typeof(TTransportOptions), _routes.ToArray(), _consumers.ToArray(), _topologyMode,
            (provider, failures) =>
            {
                try { _ = provider.GetRequiredService<IOptionsMonitor<TTransportOptions>>().Get(Name); }
                catch (OptionsValidationException error)
                {
                    foreach (var failure in error.Failures) failures.Add($"Transport '{Name}': {failure}");
                }
                catch (InvalidOperationException error)
                {
                    // Configuration binding uses InvalidOperationException for malformed values.
                    failures.Add($"Transport '{Name}': {error.Message}");
                }
            });
    }
}
