using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics.CodeAnalysis;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer;

public static partial class SqlServerOutboxServiceCollectionExtensions
{
    /// <summary>Registers leased unordered recovery and bounded dead-row/inbox retention against existing tables.</summary>
    /// <remarks>Use the same schema and prefix as the writer. Ordered recovery requires the ordered lane dispatcher.</remarks>
    [RequiresUnreferencedCode("SqlClient runtime trimming is unsupported.")]
    [RequiresDynamicCode("SqlClient runtime Native AOT is unsupported.")]
    public static IServiceCollection AddRelayerSqlServerMaintenance(this IServiceCollection services,
        string name, string connectionString, SqlServerMaintenanceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = SqlServerDirectDispatcher.ValidateConnectionString(connectionString);
        options ??= new();
        _ = options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(SqlServerMaintenanceWorker) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"SQL Server maintenance worker '{name}' is already registered.");
        services.AddOptions<SqlServerMaintenanceOptions>(name);
        services.AddSingleton(new MaintenanceRegistration(name, options));
        services.TryAddSingleton<IOptionsFactory<SqlServerMaintenanceOptions>>(provider => new MaintenanceOptionsFactory(
            provider.GetServices<MaintenanceRegistration>(), provider.GetServices<IConfigureOptions<SqlServerMaintenanceOptions>>(),
            provider.GetServices<IPostConfigureOptions<SqlServerMaintenanceOptions>>(), provider.GetServices<IValidateOptions<SqlServerMaintenanceOptions>>()));
        services.AddKeyedSingleton<SqlServerMaintenanceWorker>(name, (provider, _) => new(connectionString,
            provider.GetRequiredService<IOptionsMonitor<SqlServerMaintenanceOptions>>().Get(name), provider.GetServices<ITransport>(),
            provider.GetRequiredService<ILogger<SqlServerMaintenanceWorker>>(), provider.GetRequiredService<ILogger<SqlServerDirectDispatcher>>(),
            provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<SqlServerMaintenanceWorker>(name));
        return services;
    }

    /// <summary>Snapshots maintenance settings from configuration without runtime reflection.</summary>
    [RequiresUnreferencedCode("SqlClient runtime trimming is unsupported.")]
    [RequiresDynamicCode("SqlClient runtime Native AOT is unsupported.")]
    public static IServiceCollection AddRelayerSqlServerMaintenance(this IServiceCollection services,
        string name, string connectionString, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new SqlServerMaintenanceOptions();
        return services.AddRelayerSqlServerMaintenance(name, connectionString, new SqlServerMaintenanceOptions
        {
            Schema = configuration[nameof(defaults.Schema)],
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

    private sealed record MaintenanceRegistration(string Name, SqlServerMaintenanceOptions Options);

    private sealed class MaintenanceOptionsFactory(IEnumerable<MaintenanceRegistration> registrations,
        IEnumerable<IConfigureOptions<SqlServerMaintenanceOptions>> setups,
        IEnumerable<IPostConfigureOptions<SqlServerMaintenanceOptions>> postConfigures,
        IEnumerable<IValidateOptions<SqlServerMaintenanceOptions>> validations)
        : OptionsFactory<SqlServerMaintenanceOptions>(setups, postConfigures, validations)
    {
        protected override SqlServerMaintenanceOptions CreateInstance(string name)
        {
            foreach (var registration in registrations)
                if (registration.Name == name) return registration.Options;
            return new();
        }
    }
}
