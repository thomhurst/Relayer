using Microsoft.Extensions.DependencyInjection;

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

        services.AddSingleton(builder.MessageTypes);
        services.AddSingleton<IRelayerBuilder>(builder);

        return services;
    }
}
