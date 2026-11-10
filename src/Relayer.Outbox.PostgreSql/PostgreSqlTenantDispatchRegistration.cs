using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Hosting;

namespace Relayer.Outbox.PostgreSql;

public static partial class PostgreSqlOutboxServiceCollectionExtensions
{
    /// <summary>Runs one bounded scheduler over an application-owned ITenantSource catalog, without per-tenant hosted workers.</summary>
    /// <remarks>Register ITenantSource and native transports first. Do not also register fixed dispatch workers for catalog stores.</remarks>
    public static IServiceCollection AddRelayerPostgreSqlTenantDispatch(this IServiceCollection services,
        PostgreSqlTenantDispatchOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        options ??= new();
        options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(PostgreSqlTenantDispatcher))
                throw new InvalidOperationException("The PostgreSQL tenant dispatcher is already registered.");
        services.AddSingleton(options);
        services.AddSingleton<IOptions<PostgreSqlTenantDispatchOptions>>(Options.Create(options));
        services.AddSingleton<PostgreSqlTenantDispatcher>();
        services.AddSingleton<IHostedService>(static provider => provider.GetRequiredService<PostgreSqlTenantDispatcher>());
        return services;
    }
    /// <summary>Snapshots immutable tenant scheduler limits from configuration without runtime reflection.</summary>
    public static IServiceCollection AddRelayerPostgreSqlTenantDispatch(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new PostgreSqlTenantDispatchOptions();
        return services.AddRelayerPostgreSqlTenantDispatch(new PostgreSqlTenantDispatchOptions
        {
            MaxConnections = Read(nameof(defaults.MaxConnections), defaults.MaxConnections),
            PollInterval = Read(nameof(defaults.PollInterval), defaults.PollInterval),
            TurnTimeout = Read(nameof(defaults.TurnTimeout), defaults.TurnTimeout),
            BatchSize = Read(nameof(defaults.BatchSize), defaults.BatchSize),
            QueueCapacity = Read(nameof(defaults.QueueCapacity), defaults.QueueCapacity),
            RecoveryDelay = Read(nameof(defaults.RecoveryDelay), defaults.RecoveryDelay),
            CleanupInterval = Read(nameof(defaults.CleanupInterval), defaults.CleanupInterval),
            DeadRetention = Retention(nameof(defaults.DeadRetention), defaults.DeadRetention),
            InboxRetention = Retention(nameof(defaults.InboxRetention), defaults.InboxRetention)
        });
        T Read<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var result)) return result;
            throw new ArgumentException($"Tenant dispatch {key}: '{value}' is invalid.", nameof(configuration));
        }
        TimeSpan? Retention(string key, TimeSpan? fallback) =>
            string.Equals(configuration[key], "infinite", StringComparison.OrdinalIgnoreCase)
                ? null : Read(key, fallback!.Value);
    }

}
