using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Outbox.MySql;

public static partial class MySqlOutboxServiceCollectionExtensions
{
    /// <summary>Registers leased unordered recovery and bounded dead-row/inbox retention against existing tables.</summary>
    /// <remarks>Use the same database and prefix as the writer. Ordered recovery requires the ordered lane dispatcher.</remarks>
    public static IServiceCollection AddRelayerMySqlMaintenance(this IServiceCollection services,
        string name, MySqlConnector.MySqlDataSource dataSource, MySqlMaintenanceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(dataSource);
        MySqlMaintenanceWorker.ValidateDataSource(dataSource);
        options ??= new();
        _ = options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(MySqlMaintenanceWorker) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"MySQL maintenance worker '{name}' is already registered.");
        services.AddOptions<MySqlMaintenanceOptions>(name);
        services.AddSingleton(new MaintenanceRegistration(name, options));
        services.TryAddSingleton<IOptionsFactory<MySqlMaintenanceOptions>>(provider => new MaintenanceOptionsFactory(
            provider.GetServices<MaintenanceRegistration>(), provider.GetServices<IConfigureOptions<MySqlMaintenanceOptions>>(),
            provider.GetServices<IPostConfigureOptions<MySqlMaintenanceOptions>>(), provider.GetServices<IValidateOptions<MySqlMaintenanceOptions>>()));
        services.AddKeyedSingleton<MySqlMaintenanceWorker>(name, (provider, _) => new(dataSource,
            provider.GetRequiredService<IOptionsMonitor<MySqlMaintenanceOptions>>().Get(name), provider.GetServices<ITransport>(),
            provider.GetRequiredService<ILogger<MySqlMaintenanceWorker>>(), provider.GetRequiredService<ILogger<MySqlDirectDispatcher>>(),
            provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<MySqlMaintenanceWorker>(name));
        return services;
    }

    /// <summary>Snapshots maintenance settings from configuration without runtime reflection.</summary>
    public static IServiceCollection AddRelayerMySqlMaintenance(this IServiceCollection services,
        string name, MySqlConnector.MySqlDataSource dataSource, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new MySqlMaintenanceOptions();
        return services.AddRelayerMySqlMaintenance(name, dataSource, new MySqlMaintenanceOptions
        {
            TablePrefix = configuration[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            MaxDispatchAttempts = ReadValue(nameof(defaults.MaxDispatchAttempts), defaults.MaxDispatchAttempts),
            BatchSize = ReadValue(nameof(defaults.BatchSize), defaults.BatchSize),
            RecoveryDelay = ReadValue(nameof(defaults.RecoveryDelay), defaults.RecoveryDelay),
            SweepInterval = ReadValue(nameof(defaults.SweepInterval), defaults.SweepInterval),
            LeaseDuration = ReadValue(nameof(defaults.LeaseDuration), defaults.LeaseDuration),
            ClaimDuration = ReadValue(nameof(defaults.ClaimDuration), defaults.ClaimDuration),
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

    private sealed record MaintenanceRegistration(string Name, MySqlMaintenanceOptions Options);

    private sealed class MaintenanceOptionsFactory(IEnumerable<MaintenanceRegistration> registrations,
        IEnumerable<IConfigureOptions<MySqlMaintenanceOptions>> setups,
        IEnumerable<IPostConfigureOptions<MySqlMaintenanceOptions>> postConfigures,
        IEnumerable<IValidateOptions<MySqlMaintenanceOptions>> validations)
        : OptionsFactory<MySqlMaintenanceOptions>(setups, postConfigures, validations)
    {
        protected override MySqlMaintenanceOptions CreateInstance(string name)
        {
            foreach (var registration in registrations)
                if (registration.Name == name) return registration.Options;
            return new();
        }
    }
}
