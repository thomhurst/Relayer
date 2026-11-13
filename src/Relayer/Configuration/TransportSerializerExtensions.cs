using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer;

/// <summary>Explicit serializer selection for a named transport, resolved once during pipeline composition.</summary>
public static class TransportSerializerExtensions
{
    /// <summary>Uses a DI-owned serializer for all sends and consumers on this transport.</summary>
    public static TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions> UseSerializer<TTransportOptions, TRouteOptions, TConsumerOptions>(
        this TransportBuilder<TTransportOptions, TRouteOptions, TConsumerOptions> builder,
        Func<IServiceProvider, IMessageSerializer> factory)
        where TTransportOptions : class, new()
        where TRouteOptions : class, new()
        where TConsumerOptions : class, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        builder.RegisterSerializer(factory);
        return builder;
    }
}
