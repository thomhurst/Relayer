using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer;

/// <summary>Registers Relayer with dependency injection.</summary>
public static class RelayerServiceCollectionExtensions
{
    /// <summary>Adds Relayer and configures it. Call once per service collection.</summary>
    /// <exception cref="InvalidOperationException">Relayer has already been added.</exception>
    public static IServiceCollection AddRelayer(this IServiceCollection services, Action<IRelayerBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        if (services.Any(d => d.ServiceType == typeof(IRelayerBuilder)))
        {
            throw new InvalidOperationException("AddRelayer has already been called. Configure everything in a single call.");
        }

        var builder = new RelayerBuilder(services);
        configure(builder);
        builder.MessageTypes.Freeze();

        if (!services.Any(d => d.ServiceType == typeof(IMessageSerializer)))
        {
            var contexts = services.Where(d => d.ServiceType == typeof(JsonSerializerRegistration))
                .Select(d => ((JsonSerializerRegistration)d.ImplementationInstance!).Context).ToArray();
            services.AddSingleton<IMessageSerializer>(new JsonMessageSerializer(builder.MessageTypes.CreateJsonTypes(contexts)));
        }

        services.AddSingleton(builder.MessageTypes);
        services.AddSingleton<IRelayerBuilder>(builder);

        return services;
    }
}
