using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

/// <summary>Registers explicitly named MySQL durable read/claim stores.</summary>
public static partial class MySqlOutboxServiceCollectionExtensions
{
    /// <summary>Registers a scoped keyed store. The data source remains application-owned.</summary>
    /// <remarks>Does not create tables, register an enlistment or start workers.</remarks>
    public static IServiceCollection AddRelayerMySqlOutboxStore(this IServiceCollection services,
        string name, MySqlDataSource dataSource, MySqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(dataSource);
        MySqlOutboxStore.ValidateDataSource(dataSource);
        var snapshot = new MySqlOutboxOptions { MaxDispatchAttempts = (options ?? new()).MaxDispatchAttempts, TablePrefix = (options ?? new()).TablePrefix };
        _ = MySqlOutboxSchema.Table(snapshot);
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(IOutboxStore) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"Outbox store '{name}' is already registered.");
        services.AddKeyedScoped<IOutboxStore>(name, (_, _) => new MySqlOutboxStore(dataSource, snapshot));
        return services;
    }
}
