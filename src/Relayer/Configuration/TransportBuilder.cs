using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Relayer.Pipeline;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer;

/// <summary>Registers a consumer through a generated handler declaration.</summary>
public interface ITransportConsumerBuilder
{
    /// <summary>Configures the handler for a native source using the transport defaults.</summary>
    void Consume(string source, Action<IConsumerBuilder> configure);
}

/// <summary>The common declaration surface used by transport-specific extension methods.</summary>
public sealed class TransportBuilder<TTransportOptions, TConsumerOptions> : ITransportConsumerBuilder
    where TTransportOptions : class, new()
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
    private Func<string, string>? _normalizeRouteDestination;
    private bool _normalizeConsumerSources;

    internal TransportBuilder(IServiceCollection services, HandlerRegistry handlers, string name)
    {
        _services = services;
        _handlers = handlers;
        Name = name;
        Options = services.AddOptions<TTransportOptions>(name);
    }

    /// <summary>The transport's case-sensitive registration and Options name.</summary>
    public string Name
    {
        get;
    }

    /// <summary>Overrides the Relayer topology mode for this transport; null inherits the default.</summary>
    public TopologyMode? TopologyMode
    {
        get
        {
            return _topologyMode;
        }
        set
        {
            EnsureMutable();
            _topologyMode = value;
        }
    }

    /// <summary>Named Options pattern configuration, binding, validation and reload registration.</summary>
    public OptionsBuilder<TTransportOptions> Options
    {
        get;
    }

    /// <summary>Default portable send settings for routes declared afterward. A route may replace them with SendOptions.</summary>
    public SendRouteOptions SendDefaults
    {
        get
        {
            return _sendDefaults;
        }
        set
        {
            EnsureMutable();
            ArgumentNullException.ThrowIfNull(value);
            _sendDefaults = value;
        }
    }

    /// <summary>Declares a message destination. Whether this sends or publishes is a native routing concern.</summary>
    public TransportBuilder<TTransportOptions, TConsumerOptions> Send<TMessage>(string destination,
        Action<RouteBuilder<TMessage>>? configure = null)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(destination);
        _routes.Add(BuildSendRoute(destination, configure));
        return this;
    }

    /// <summary>Declares a message type sent to one of several native destinations, chosen from each message at send time.</summary>
    /// <remarks>
    /// Each declared destination is validated and provisioned at startup like a single route. A send whose selector returns
    /// a destination that is not declared here fails. Replies and tombstones require a single-destination route.
    /// </remarks>
    public TransportBuilder<TTransportOptions, TConsumerOptions> Send<TMessage>(IEnumerable<string> destinations,
        Func<TMessage, string> destination, Action<RouteBuilder<TMessage>>? configure = null)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(destinations);
        ArgumentNullException.ThrowIfNull(destination);
        var declared = destinations.ToArray();

        if (declared.Length == 0)
        {
            throw new ArgumentException("Declare at least one destination.", nameof(destinations));
        }

        if (Array.IndexOf(declared, null) >= 0)
        {
            throw new ArgumentNullException(nameof(destinations), "A destination must not be null.");
        }

        if (declared.Distinct(StringComparer.Ordinal).Count() != declared.Length)
        {
            throw new ArgumentException("Declare each destination once.", nameof(destinations));
        }

        var route = new RouteBuilder<TMessage>(declared[0]);
        route.SendOptions = _sendDefaults;
        configure?.Invoke(route);
        _routes.AddRange(route.Build(declared, destination));
        return this;
    }

    /// <summary>Declares a request send route and a dedicated, process-local reply consumer.</summary>
    /// <remarks>
    /// Give each requesting process its own reply consumer at <paramref name="replyAddress"/>.
    /// Set <see cref="RequestBuilder{TRequest,TConsumerOptions}.ReplyDestination"/> when publishing replies requires a different address.
    /// </remarks>
    public TransportBuilder<TTransportOptions, TConsumerOptions> Request<TRequest, TResponse>(
        string destination, string replyAddress,
        Action<RequestBuilder<TRequest, TConsumerOptions>>? configure = null)
    {
        EnsureMutable();
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentException.ThrowIfNullOrWhiteSpace(replyAddress);
        var request = new RequestBuilder<TRequest, TConsumerOptions>();
        configure?.Invoke(request);
        var replyDestination = request.ReplyDestination ?? replyAddress;
        ArgumentException.ThrowIfNullOrWhiteSpace(replyDestination, nameof(request.ReplyDestination));
        var options = request.Options;
        options.Validate();

        var route = BuildSendRoute(destination, request.ConfigureSend);
        var consumer = BuildConsumer(replyAddress, replies =>
        {
            request.ConfigureReplies?.Invoke(replies);
            ((IConsumerBuilder)replies).AddHandler(new ReplyDescriptor<TRequest, TResponse>());
        });

        RegisterRequestServices<TRequest, TResponse>(replyAddress, replyDestination, options);
        _routes.Add(route);
        _consumers.Add(consumer);
        return this;
    }

    /// <summary>Declares a consumer on a native source endpoint.</summary>
    public TransportBuilder<TTransportOptions, TConsumerOptions> Consume(string source,
        Action<ConsumerBuilder<TConsumerOptions>> configure)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(configure);
        _consumers.Add(BuildConsumer(source, configure));
        return this;
    }

    void ITransportConsumerBuilder.Consume(string source, Action<IConsumerBuilder> configure)
    {
        Consume(source, configure);
    }

    private IRouteDefinition BuildSendRoute<TMessage>(string destination, Action<RouteBuilder<TMessage>>? configure)
    {
        var route = new RouteBuilder<TMessage>(destination);
        route.SendOptions = _sendDefaults;
        configure?.Invoke(route);
        return route.Build();
    }

    private IConsumerDefinition BuildConsumer(string source, Action<ConsumerBuilder<TConsumerOptions>> configure)
    {
        var consumer = new ConsumerBuilder<TConsumerOptions>(_services, _handlers, source);
        configure(consumer);
        return consumer.Build();
    }

    private void RegisterRequestServices<TRequest, TResponse>(string replySource, string replyDestination, RequestOptions options)
    {
        if (_services.Any(static service => service.ServiceType == typeof(IRequestClient<TRequest, TResponse>) && !service.IsKeyedService))
        {
            throw new InvalidOperationException("This request/response pair already has a registration. Declare it once.");
        }

        string transportName = Name;
        _services.AddSingleton<RequestReplies<TRequest, TResponse>>(_ => new(transportName, replySource, replyDestination, options));
        _services.AddScoped<IRequestClient<TRequest, TResponse>, RequestClient<TRequest, TResponse>>();
        _services.AddSingleton<IRequestRegistration, RequestRegistration<TRequest, TResponse>>();
    }

    private void EnsureMutable()
    {
        if (_built)
        {
            throw new InvalidOperationException("The transport declaration is frozen.");
        }
    }

    internal void RegisterSerializer(Func<IServiceProvider, IMessageSerializer> factory, bool ownsSerializer)
    {
        EnsureMutable();

        if (_hasSerializer)
        {
            throw new InvalidOperationException($"Transport '{Name}' already has a serializer. Select one serializer per transport.");
        }

        _hasSerializer = true;
        NamedMessageSerializer.Register(_services, Name, factory, ownsSerializer);
    }

    internal void SetRouteDestinationNormalizer(Func<string, string> normalize, bool normalizeConsumerSources = false)
    {
        EnsureMutable();
        _normalizeRouteDestination = normalize;
        _normalizeConsumerSources = normalizeConsumerSources;
    }

    internal TransportDefinition Build()
    {
        _built = true;
        return new TransportDefinition(Name, typeof(TTransportOptions), _routes.ToArray(), _consumers.ToArray(), _topologyMode,
            (provider, failures) =>
            {
                try
                {
                    _ = provider.GetRequiredService<IOptionsMonitor<TTransportOptions>>().Get(Name);
                }
                catch (OptionsValidationException error)
                {
                    foreach (var failure in error.Failures)
                    {
                        failures.Add($"Transport '{Name}': {failure}");
                    }
                }
                catch (InvalidOperationException error)
                {
                    // Configuration binding uses InvalidOperationException for malformed values.
                    failures.Add($"Transport '{Name}': {error.Message}");
                }
            }, _normalizeRouteDestination, _normalizeConsumerSources);
    }
}
