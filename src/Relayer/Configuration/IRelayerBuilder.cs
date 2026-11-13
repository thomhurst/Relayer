using Relayer.Transports;
using Relayer.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Configures Relayer. Transports and features add extension methods to this builder.</summary>
public interface IRelayerBuilder
{
    /// <summary>The application's service collection.</summary>
    IServiceCollection Services { get; }

    /// <summary>How broker entities are treated at startup. Defaults to <see cref="TopologyMode.Validate"/>.</summary>
    TopologyMode TopologyMode { get; set; }
}

internal sealed class RelayerBuilder(IServiceCollection services) : IRelayerBuilder, IRelayerInfrastructure
{
    private TopologyMode _topologyMode = TopologyMode.Validate;
    public IServiceCollection Services { get; } = services;

    public MessageTypeRegistry MessageTypes { get; } = new();

    public HandlerRegistry Handlers { get; } = new();

    public UpcasterRegistry Upcasters { get; } = new();

    public TopologyMode TopologyMode
    {
        get => _topologyMode;
        set
        {
            EnsureMutable();
            _topologyMode = value;
        }
    }

    internal List<TransportDefinition> Transports { get; } = [];

    public IRelayerBuilder AddTransport<TTransportOptions, TRouteOptions, TConsumerOptions>(string name,
        Action<TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions>> configure)
        where TTransportOptions : class, new()
        where TRouteOptions : class, new()
        where TConsumerOptions : class, new()
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(configure);
        EnsureMutable();

        var transport = new TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions>(Services, Handlers, name);
        configure(transport);
        Transports.Add(transport.Build());
        return this;
    }

    private void EnsureMutable()
    {
        if (MessageTypes.IsFrozen)
            throw new InvalidOperationException("Relayer configuration is frozen after AddRelayer returns.");
    }
}
