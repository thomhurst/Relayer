using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

public static partial class PostgreSqlOutboxServiceCollectionExtensions
{
    /// <summary>Dispatches committed unordered rows using native PostgreSQL logical replication notifications.</summary>
    /// <remarks>
    /// The factory must return a new native replication connection to the writer's database; the worker owns it.
    /// Provision the slot and INSERT-only publication first. Register maintenance for retries, expired claims,
    /// future schedules and cleanup, and the ordered dispatcher for ordered rows. The data source stays application-owned.
    /// </remarks>
    public static IServiceCollection AddRelayerPostgreSqlChangeCapture(this IServiceCollection services,
        string name, NpgsqlDataSource dataSource, Func<LogicalReplicationConnection> connectionFactory,
        PostgreSqlChangeCaptureOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(connectionFactory);
        ArgumentNullException.ThrowIfNull(options);
        _ = options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(PostgreSqlChangeCaptureWorker) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"PostgreSQL CDC worker '{name}' is already registered.");
        services.AddOptions<PostgreSqlChangeCaptureOptions>(name);
        services.AddSingleton(new ChangeCaptureRegistration(name, options));
        services.TryAddSingleton<IOptionsFactory<PostgreSqlChangeCaptureOptions>>(provider => new ChangeCaptureOptionsFactory(
            provider.GetServices<ChangeCaptureRegistration>(), provider.GetServices<IConfigureOptions<PostgreSqlChangeCaptureOptions>>(),
            provider.GetServices<IPostConfigureOptions<PostgreSqlChangeCaptureOptions>>(), provider.GetServices<IValidateOptions<PostgreSqlChangeCaptureOptions>>()));
        services.AddKeyedSingleton<PostgreSqlChangeCaptureWorker>(name, (provider, _) => new(dataSource, connectionFactory,
            provider.GetRequiredService<IOptionsMonitor<PostgreSqlChangeCaptureOptions>>().Get(name),
            provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<PostgreSqlChangeCaptureWorker>>(),
            provider.GetRequiredService<ILogger<PostgreSqlMaintenanceWorker>>(), provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>(),
            provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<PostgreSqlChangeCaptureWorker>(name));
        return services;
    }

    /// <summary>Snapshots store and retry settings from configuration while preserving native replication settings.</summary>
    public static IServiceCollection AddRelayerPostgreSqlChangeCapture(this IServiceCollection services,
        string name, NpgsqlDataSource dataSource, Func<LogicalReplicationConnection> connectionFactory,
        PgOutputReplicationSlot slot, PgOutputReplicationOptions replication, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new PostgreSqlChangeCaptureOptions { Slot = slot, Replication = replication };
        return services.AddRelayerPostgreSqlChangeCapture(name, dataSource, connectionFactory, new PostgreSqlChangeCaptureOptions
        {
            Slot = slot, Replication = replication,
            Schema = configuration[nameof(defaults.Schema)],
            TablePrefix = configuration[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            MaxDispatchAttempts = ReadValue(nameof(defaults.MaxDispatchAttempts), defaults.MaxDispatchAttempts),
            BatchSize = ReadValue(nameof(defaults.BatchSize), defaults.BatchSize),
            ClaimDuration = ReadValue(nameof(defaults.ClaimDuration), defaults.ClaimDuration),
            ReconnectDelay = ReadValue(nameof(defaults.ReconnectDelay), defaults.ReconnectDelay)
        });

        T ReadValue<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var result)) return result;
            throw new ArgumentException($"CDC {key}: '{value}' is invalid.", nameof(configuration));
        }
    }

    private sealed record ChangeCaptureRegistration(string Name, PostgreSqlChangeCaptureOptions Options);

    private sealed class ChangeCaptureOptionsFactory(IEnumerable<ChangeCaptureRegistration> registrations,
        IEnumerable<IConfigureOptions<PostgreSqlChangeCaptureOptions>> setups,
        IEnumerable<IPostConfigureOptions<PostgreSqlChangeCaptureOptions>> postConfigures,
        IEnumerable<IValidateOptions<PostgreSqlChangeCaptureOptions>> validations)
        : OptionsFactory<PostgreSqlChangeCaptureOptions>(setups, postConfigures, validations)
    {
        protected override PostgreSqlChangeCaptureOptions CreateInstance(string name)
        {
            foreach (var registration in registrations)
                if (registration.Name == name) return registration.Options;
            throw new InvalidOperationException($"PostgreSQL CDC worker '{name}' is not registered.");
        }
    }
}
