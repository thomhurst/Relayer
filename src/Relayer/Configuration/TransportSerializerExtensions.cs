using Relayer.Serialization;

namespace Relayer;

/// <summary>Explicit serializer selection for a named transport, resolved once during pipeline composition.</summary>
public static class TransportSerializerExtensions
{
    /// <summary>Uses a serializer for all sends and consumers on this transport and transfers disposal to the host.</summary>
    /// <remarks>Use <c>UseSerializerFromServices</c> to select a singleton with existing disposal ownership.</remarks>
    public static TransportBuilder<TTransportOptions, TConsumerOptions> UseSerializer<TTransportOptions, TConsumerOptions>(
        this TransportBuilder<TTransportOptions, TConsumerOptions> builder,
        Func<IServiceProvider, IMessageSerializer> factory)
        where TTransportOptions : class, new()
        where TConsumerOptions : class, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(factory);
        builder.RegisterSerializer(factory, ownsSerializer: true);
        return builder;
    }

    /// <summary>Selects an existing singleton for all sends and consumers on this transport.</summary>
    /// <remarks>The original registration controls disposal. The resolver must return an existing singleton.</remarks>
    public static TransportBuilder<TTransportOptions, TConsumerOptions> UseSerializerFromServices<TTransportOptions, TConsumerOptions>(
        this TransportBuilder<TTransportOptions, TConsumerOptions> builder,
        Func<IServiceProvider, IMessageSerializer> resolve)
        where TTransportOptions : class, new()
        where TConsumerOptions : class, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(resolve);
        builder.RegisterSerializer(resolve, ownsSerializer: false);
        return builder;
    }
}
