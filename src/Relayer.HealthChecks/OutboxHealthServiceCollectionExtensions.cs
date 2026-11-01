using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Relayer.HealthChecks;

/// <summary>Connects explicitly named durable stores to the optional outbox backlog check.</summary>
public static class OutboxHealthServiceCollectionExtensions
{
    /// <summary>Registers a scoped health provider backed by the keyed IOutboxStore with the same name.</summary>
    /// <remarks>
    /// Also call AddRelayerHealthChecks and list every expected store in RelayerHealthCheckOptions.OutboxNames.
    /// Register the keyed store separately; this method neither creates database connections nor enables dispatch.
    /// Repeated calls preserve an existing health provider for this name. Other names remain independent.
    /// </remarks>
    public static IServiceCollection AddRelayerOutboxHealthProvider(this IServiceCollection services, string storeName)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        if (!services.Any(descriptor => descriptor.ImplementationInstance is OutboxHealthRegistration registration &&
            string.Equals(registration.StoreName, storeName, StringComparison.Ordinal)))
            services.AddSingleton(new OutboxHealthRegistration(storeName));
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddKeyedScoped<IOutboxHealthProvider>(storeName, static (provider, key) =>
            new OutboxStoreHealthProvider(provider.GetRequiredKeyedService<IOutboxStore>(key),
                provider.GetRequiredService<TimeProvider>()));
        return services;
    }
}

internal sealed record OutboxHealthRegistration(string StoreName);
