using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

/// <summary>Configures Relayer. Transports and features add extension methods to this builder.</summary>
public interface IRelayerBuilder
{
    /// <summary>The application's service collection.</summary>
    IServiceCollection Services { get; }

    /// <summary>The message type registry.</summary>
    MessageTypeRegistry MessageTypes { get; }

    /// <summary>The explicitly registered, source-generated handler descriptors.</summary>
    HandlerRegistry Handlers { get; }

    /// <summary>How broker entities are treated at startup. Defaults to <see cref="TopologyMode.Validate"/>.</summary>
    TopologyMode TopologyMode { get; set; }
}

internal sealed class RelayerBuilder(IServiceCollection services) : IRelayerBuilder
{
    public IServiceCollection Services { get; } = services;

    public MessageTypeRegistry MessageTypes { get; } = new();

    public HandlerRegistry Handlers { get; } = new();

    public TopologyMode TopologyMode { get; set; } = TopologyMode.Validate;
}
