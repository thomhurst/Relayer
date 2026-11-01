using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Registers explicitly named PostgreSQL durable read/claim stores.</summary>
public static class PostgreSqlOutboxServiceCollectionExtensions
{
    /// <summary>Registers a named durable store and a hosted worker for unordered, commit-triggered sends.</summary>
    /// <remarks>
    /// Select this name with EntityFrameworkCoreOutboxOptions.DispatchStoreName on participating contexts.
    /// The data source remains application-owned. This does not dispatch ordered or future-due rows or run recovery polling.
    /// </remarks>
    public static IServiceCollection AddRelayerPostgreSqlDirectDispatch(this IServiceCollection services,
        string storeName, Npgsql.NpgsqlDataSource dataSource, PostgreSqlOutboxOptions? storeOptions = null,
        PostgreSqlDirectDispatchOptions? dispatchOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentNullException.ThrowIfNull(dataSource);
        if (services.Any(descriptor => descriptor.ServiceType == typeof(PostgreSqlDirectDispatcher) && Equals(descriptor.ServiceKey, storeName)))
            throw new InvalidOperationException($"Direct outbox dispatch store '{storeName}' is already registered.");
        dispatchOptions ??= new();
        if (dispatchOptions.QueueCapacity is <= 0 or int.MaxValue || dispatchOptions.BatchSize <= 0 || dispatchOptions.ClaimDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(dispatchOptions), "QueueCapacity and BatchSize must be positive, QueueCapacity must be below Int32.MaxValue, and ClaimDuration must be positive.");
        storeOptions ??= new();
        var snapshot = new PostgreSqlOutboxOptions { Schema = storeOptions.Schema, TablePrefix = storeOptions.TablePrefix };
        _ = PostgreSqlOutboxSchema.CreateScript(snapshot);
        services.AddRelayerPostgreSqlOutboxStore(storeName, dataSource, snapshot);
        services.AddSingleton(new DispatchSettings(storeName, dispatchOptions));
        services.AddOptions<PostgreSqlDirectDispatchOptions>();
        services.TryAddSingleton<IOptionsFactory<PostgreSqlDirectDispatchOptions>, DispatchOptionsFactory>();
        services.AddKeyedSingleton<PostgreSqlDirectDispatcher>(storeName, (provider, _) =>
            new PostgreSqlDirectDispatcher(dataSource, snapshot,
                provider.GetRequiredService<IOptionsMonitor<PostgreSqlDirectDispatchOptions>>().Get(storeName),
                provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>(storeName));
        return services;
    }

    /// <summary>Binds immutable direct-dispatch limits from configuration at registration.</summary>
    public static IServiceCollection AddRelayerPostgreSqlDirectDispatch(this IServiceCollection services,
        string storeName, Npgsql.NpgsqlDataSource dataSource, IConfiguration configuration,
        PostgreSqlOutboxOptions? storeOptions = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new PostgreSqlDirectDispatchOptions();
        return services.AddRelayerPostgreSqlDirectDispatch(storeName, dataSource, storeOptions, new PostgreSqlDirectDispatchOptions
        {
            QueueCapacity = Read(configuration, nameof(defaults.QueueCapacity), defaults.QueueCapacity),
            BatchSize = Read(configuration, nameof(defaults.BatchSize), defaults.BatchSize),
            ClaimDuration = Read(configuration, nameof(defaults.ClaimDuration), defaults.ClaimDuration)
        });
    }

    private static T Read<T>(IConfiguration configuration, string key, T fallback) where T : IParsable<T>
    {
        var value = configuration[key];
        if (value is null) return fallback;
        if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed)) return parsed;
        throw new OptionsValidationException(Options.DefaultName, typeof(PostgreSqlDirectDispatchOptions), [$"{key}: '{value}' is invalid."]);
    }

    private sealed record DispatchSettings(string Name, PostgreSqlDirectDispatchOptions Options);

    private sealed class DispatchOptionsFactory(IEnumerable<DispatchSettings> settings,
        IEnumerable<IConfigureOptions<PostgreSqlDirectDispatchOptions>> setups,
        IEnumerable<IPostConfigureOptions<PostgreSqlDirectDispatchOptions>> postConfigures,
        IEnumerable<IValidateOptions<PostgreSqlDirectDispatchOptions>> validations)
        : OptionsFactory<PostgreSqlDirectDispatchOptions>(setups, postConfigures, validations)
    {
        protected override PostgreSqlDirectDispatchOptions CreateInstance(string name)
        {
            foreach (var setting in settings)
                if (setting.Name == name) return setting.Options;
            return new();
        }
    }

    /// <summary>Registers a keyed scoped store with an application-owned data source and snapshotted names.</summary>
    /// <remarks>This does not create tables, enlist sends, register health checks or run dispatch workers.</remarks>
    public static IServiceCollection AddRelayerPostgreSqlOutboxStore(this IServiceCollection services,
        string storeName, Npgsql.NpgsqlDataSource dataSource, PostgreSqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new();
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        var snapshot = new PostgreSqlOutboxOptions { Schema = options.Schema, TablePrefix = options.TablePrefix };
        // Validate identifiers immediately, rather than waiting for the first database operation.
        _ = new PostgreSqlOutboxStore(dataSource, snapshot);
        services.AddKeyedScoped<IOutboxStore>(storeName, (_, _) => new PostgreSqlOutboxStore(dataSource, snapshot));
        return services;
    }
}
