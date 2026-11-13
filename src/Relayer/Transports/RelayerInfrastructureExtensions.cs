using Relayer.Pipeline;

namespace Relayer.Transports;

/// <summary>Registration services used by generated code and extension authors.</summary>
public interface IRelayerInfrastructure
{
    /// <summary>The explicitly registered wire identities.</summary>
    MessageTypeRegistry MessageTypes { get; }
    /// <summary>The source-generated handler descriptors.</summary>
    HandlerRegistry Handlers { get; }
    /// <summary>The explicitly registered message upgrades.</summary>
    UpcasterRegistry Upcasters { get; }

    /// <summary>Declares a named transport. Transport packages wrap this method in typed extension methods.</summary>
    IRelayerBuilder AddTransport<TTransportOptions, TConsumerOptions>(string name,
        Action<TransportBuilder<TTransportOptions, TConsumerOptions>> configure)
        where TTransportOptions : class, new()
        where TConsumerOptions : class, new();
}

/// <summary>Access to registration infrastructure outside the application builder surface.</summary>
public static class RelayerInfrastructureExtensions
{
    /// <summary>Gets registration infrastructure for generated code and extensions.</summary>
    public static IRelayerInfrastructure Infrastructure(this IRelayerBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder as IRelayerInfrastructure
            ?? throw new ArgumentException("The builder must implement IRelayerInfrastructure to support registration extensions.", nameof(builder));
    }

    /// <summary>Declares a named transport from a transport package's typed extension.</summary>
    public static IRelayerBuilder AddTransport<TTransportOptions, TConsumerOptions>(this IRelayerBuilder builder,
        string name, Action<TransportBuilder<TTransportOptions, TConsumerOptions>> configure)
        where TTransportOptions : class, new()
        where TConsumerOptions : class, new()
        => builder.Infrastructure().AddTransport(name, configure);
}
