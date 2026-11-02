using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Relayer;

/// <summary>Explicit tenant propagation for scoped sends and single-message consumers.</summary>
public static class TenantServiceCollectionExtensions
{
    /// <summary>Restores the tenant before handler dependencies resolve and propagates it on scoped sends.</summary>
    /// <remarks>Batch consumers share a scope and are rejected at composition. This does not select databases or authorize tenants.</remarks>
    /// <param name="services">The application service collection.</param>
    /// <param name="contextPoolCapacity">Maximum retained contexts. Size for concurrent application and consumer scopes.</param>
    public static IServiceCollection AddRelayerTenancy(this IServiceCollection services, int contextPoolCapacity = 64)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentOutOfRangeException.ThrowIfLessThan(contextPoolCapacity, 1);
        services.TryAddSingleton<TenantContextPool>(_ => new(contextPoolCapacity));
        services.TryAddScoped<TenantContext>(static provider => provider.GetRequiredService<TenantContextPool>().Rent());
        return services;
    }
}
