using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Relayer;

/// <summary>The common declaration surface used by transport-specific extension methods.</summary>
public sealed class TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions>
    where TTransportOptions : class, new()
    where TRouteOptions : class, new()
    where TConsumerOptions : class, new()
{
    private readonly IServiceCollection _services;
    private readonly List<IRouteDefinition> _routes = [];
    private readonly List<IConsumerDefinition> _consumers = [];
    private bool _built;

    internal TransportBuilder(IServiceCollection services, string name)
    {
        _services = services;
        Name = name;
        Options = services.AddOptions<TTransportOptions>(name);
    }

    /// <summary>The transport's case-sensitive registration and Options name.</summary>
    public string Name { get; }

    /// <summary>Named Options pattern configuration, binding, validation and reload registration.</summary>
    public OptionsBuilder<TTransportOptions> Options { get; }

    /// <summary>Declares a message destination. Whether this sends or publishes is a native routing concern.</summary>
    public TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions> Send<TMessage>(string destination,
        Action<RouteBuilder<TMessage, TRouteOptions>>? configure = null)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(destination);
        var route = new RouteBuilder<TMessage, TRouteOptions>(destination);
        configure?.Invoke(route);
        _routes.Add(route.Build());
        return this;
    }

    /// <summary>Declares a consumer on a native source endpoint.</summary>
    public TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions> Consume(string source,
        Action<ConsumerBuilder<TConsumerOptions>> configure)
    {
        EnsureMutable();
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(configure);
        var consumer = new ConsumerBuilder<TConsumerOptions>(_services, source);
        configure(consumer);
        _consumers.Add(consumer.Build());
        return this;
    }

    private void EnsureMutable()
    {
        if (_built) throw new InvalidOperationException("The transport declaration is frozen.");
    }

    internal TransportDefinition Build()
    {
        _built = true;
        return new TransportDefinition(Name, typeof(TTransportOptions), _routes.ToArray(), _consumers.ToArray(),
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
