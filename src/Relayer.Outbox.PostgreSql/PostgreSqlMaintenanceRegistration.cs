using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

public static partial class PostgreSqlOutboxServiceCollectionExtensions
{
    /// <summary>Registers leased unordered recovery and bounded dead-row, sent-metadata and inbox retention against existing tables.</summary>
    /// <remarks>Use the same schema and prefix as the writer. Ordered recovery requires the ordered lane dispatcher.</remarks>
    public static IServiceCollection AddRelayerPostgreSqlMaintenance(this IServiceCollection services,
        string name, NpgsqlDataSource dataSource, PostgreSqlMaintenanceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new();
        _ = options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(PostgreSqlMaintenanceWorker) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"PostgreSQL maintenance worker '{name}' is already registered.");
        services.AddOptions<PostgreSqlMaintenanceOptions>(name);
        services.AddSingleton(new MaintenanceRegistration(name, options));
        services.AddSingleton<PostgreSqlDispatchRegistration>(provider => new(name, dataSource,
            provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get(name).Validate(), Recovery: true));
        services.TryAddSingleton<IOptionsFactory<PostgreSqlMaintenanceOptions>>(provider => new MaintenanceOptionsFactory(
            provider.GetServices<MaintenanceRegistration>(), provider.GetServices<IConfigureOptions<PostgreSqlMaintenanceOptions>>(),
            provider.GetServices<IPostConfigureOptions<PostgreSqlMaintenanceOptions>>(), provider.GetServices<IValidateOptions<PostgreSqlMaintenanceOptions>>()));
        services.AddKeyedSingleton<PostgreSqlMaintenanceWorker>(name, (provider, _) => new(dataSource,
            provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get(name), provider.GetServices<ITransport>(),
            provider.GetRequiredService<ILogger<PostgreSqlMaintenanceWorker>>(), provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>(),
            provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<PostgreSqlMaintenanceWorker>(name));
        services.AddSingleton<PostgreSqlSchedulingRegistration>(provider =>
        {
            var settings = provider.GetRequiredService<IOptionsMonitor<PostgreSqlMaintenanceOptions>>().Get(name);
            _ = settings.Validate();
            return new(settings.Schema, settings.TablePrefix);
        });
        return services;
    }

    /// <summary>Snapshots maintenance settings from configuration without runtime reflection.</summary>
    public static IServiceCollection AddRelayerPostgreSqlMaintenance(this IServiceCollection services,
        string name, NpgsqlDataSource dataSource, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new PostgreSqlMaintenanceOptions();
        return services.AddRelayerPostgreSqlMaintenance(name, dataSource, new PostgreSqlMaintenanceOptions
        {
            Schema = configuration[nameof(defaults.Schema)],
            TablePrefix = configuration[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            RetainSentMessages = ReadValue(nameof(defaults.RetainSentMessages), defaults.RetainSentMessages),
            MaxDispatchAttempts = ReadValue(nameof(defaults.MaxDispatchAttempts), defaults.MaxDispatchAttempts),
            BatchSize = ReadValue(nameof(defaults.BatchSize), defaults.BatchSize),
            RecoveryDelay = ReadValue(nameof(defaults.RecoveryDelay), defaults.RecoveryDelay),
            SweepInterval = ReadValue(nameof(defaults.SweepInterval), defaults.SweepInterval),
            LeaseDuration = ReadValue(nameof(defaults.LeaseDuration), defaults.LeaseDuration),
            ClaimDuration = ReadValue(nameof(defaults.ClaimDuration), defaults.ClaimDuration),
            SentRetention = ReadValue(nameof(defaults.SentRetention), defaults.SentRetention),
            CleanupInterval = ReadValue(nameof(defaults.CleanupInterval), defaults.CleanupInterval),
            DeadRetention = ReadRetention(nameof(defaults.DeadRetention), defaults.DeadRetention),
            InboxRetention = ReadRetention(nameof(defaults.InboxRetention), defaults.InboxRetention)
        });

        T ReadValue<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var result)) return result;
            throw new ArgumentException($"Maintenance {key}: '{value}' is invalid.", nameof(configuration));
        }
        TimeSpan? ReadRetention(string key, TimeSpan? fallback) =>
            string.Equals(configuration[key], "infinite", StringComparison.OrdinalIgnoreCase)
                ? null : ReadValue(key, fallback!.Value);
    }

    private sealed record MaintenanceRegistration(string Name, PostgreSqlMaintenanceOptions Options);

    private sealed class MaintenanceOptionsFactory(IEnumerable<MaintenanceRegistration> registrations,
        IEnumerable<IConfigureOptions<PostgreSqlMaintenanceOptions>> setups,
        IEnumerable<IPostConfigureOptions<PostgreSqlMaintenanceOptions>> postConfigures,
        IEnumerable<IValidateOptions<PostgreSqlMaintenanceOptions>> validations)
        : OptionsFactory<PostgreSqlMaintenanceOptions>(setups, postConfigures, validations)
    {
        protected override PostgreSqlMaintenanceOptions CreateInstance(string name)
        {
            foreach (var registration in registrations)
                if (registration.Name == name) return registration.Options;
            return new();
        }
    }
}
