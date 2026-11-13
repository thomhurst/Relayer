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
public static partial class PostgreSqlOutboxServiceCollectionExtensions
{
    /// <summary>Registers a keyed manager for inspecting, redriving and purging one durable outbox store.</summary>
    /// <remarks>Redrive restores pending rows for existing dispatchers. Purge discards dead rows and releases their key barriers.</remarks>
    public static IServiceCollection AddRelayerPostgreSqlDeadLetters(this IServiceCollection services,
        string storeName, Npgsql.NpgsqlDataSource dataSource, PostgreSqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new();
        _ = new PostgreSqlOutboxStore(dataSource, options);
        var snapshot = options;
        services.AddKeyedSingleton<IDeadLetterManager>(storeName, (_, _) => new PostgreSqlDeadLetterManager(storeName, dataSource, snapshot));
        return services;
    }

    /// <summary>Registers a named durable store and a hosted worker for unordered, commit-triggered sends.</summary>
    /// <remarks>
    /// Select this name with EntityFrameworkCoreOutboxOptions.DispatchStoreName on participating contexts.
    /// The data source remains application-owned. This does not dispatch ordered or future-due rows or run recovery polling.
    /// Startup requires matching AddRelayerPostgreSqlMaintenance registration for durable recovery.
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
        var snapshot = new PostgreSqlOutboxOptions { Schema = storeOptions.Schema, TablePrefix = storeOptions.TablePrefix, MaxDispatchAttempts = storeOptions.MaxDispatchAttempts, RetainSentMessages = storeOptions.RetainSentMessages };
        _ = PostgreSqlOutboxSchema.CreateScript(snapshot);
        services.AddRelayerPostgreSqlOutboxStore(storeName, dataSource, snapshot);
        services.AddSingleton(new PostgreSqlDispatchRegistration(storeName, dataSource, snapshot));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, PostgreSqlRecoveryStartupValidator>());
        services.AddSingleton(new DispatchSettings(storeName, dispatchOptions));
        services.AddOptions<PostgreSqlDirectDispatchOptions>();
        services.TryAddSingleton<IOptionsFactory<PostgreSqlDirectDispatchOptions>, DispatchOptionsFactory>();
        services.AddKeyedSingleton<PostgreSqlDirectDispatcher>(storeName, (provider, _) =>
            new PostgreSqlDirectDispatcher(dataSource, snapshot,
                provider.GetRequiredService<IOptionsMonitor<PostgreSqlDirectDispatchOptions>>().Get(storeName),
                provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<PostgreSqlDirectDispatcher>>(),
                instrumentation: provider.GetService<RelayerInstrumentation>()));
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
        var snapshot = new PostgreSqlOutboxOptions { Schema = options.Schema, TablePrefix = options.TablePrefix, MaxDispatchAttempts = options.MaxDispatchAttempts, RetainSentMessages = options.RetainSentMessages };
        // Validate identifiers immediately, rather than waiting for the first database operation.
        _ = new PostgreSqlOutboxStore(dataSource, snapshot);
        services.AddKeyedScoped<IOutboxStore>(storeName, (_, _) => new PostgreSqlOutboxStore(dataSource, snapshot));
        return services;
    }

    /// <summary>Starts one named worker for ordered rows. Deploy schema and register native transports first.</summary>
    /// <remarks>The application owns the data source. Writer and all workers must use identical schema, prefix and lane count.</remarks>
    public static IServiceCollection AddRelayerPostgreSqlOrderedDispatch(this IServiceCollection services,
        string name, Npgsql.NpgsqlDataSource dataSource, PostgreSqlOrderedDispatchOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new();
        _ = options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(PostgreSqlOrderedDispatcher) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"Ordered outbox dispatcher '{name}' is already registered.");
        var snapshot = options;
        services.AddOptions<PostgreSqlOrderedDispatchOptions>(name);
        services.AddSingleton(new OrderedRegistration(name, snapshot));
        services.TryAddSingleton<IOptionsFactory<PostgreSqlOrderedDispatchOptions>>(provider =>
            new OrderedOptionsFactory(provider.GetServices<OrderedRegistration>(), provider.GetServices<IConfigureOptions<PostgreSqlOrderedDispatchOptions>>(),
                provider.GetServices<IPostConfigureOptions<PostgreSqlOrderedDispatchOptions>>(),
                provider.GetServices<IValidateOptions<PostgreSqlOrderedDispatchOptions>>()));
        services.AddKeyedSingleton<PostgreSqlOrderedDispatcher>(name, (provider, _) =>
            new(dataSource, provider.GetRequiredService<IOptionsMonitor<PostgreSqlOrderedDispatchOptions>>().Get(name),
                provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<PostgreSqlOrderedDispatcher>>(),
                instrumentation: provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<PostgreSqlOrderedDispatcher>(name));
        services.AddSingleton<PostgreSqlSchedulingRegistration>(provider =>
        {
            var settings = provider.GetRequiredService<IOptionsMonitor<PostgreSqlOrderedDispatchOptions>>().Get(name);
            _ = settings.Validate();
            return new(settings.Schema, settings.TablePrefix, settings.Lanes);
        });
        return services;
    }

    /// <summary>Binds immutable ordered-dispatch settings without reflection, including under Native AOT.</summary>
    public static IServiceCollection AddRelayerPostgreSqlOrderedDispatch(this IServiceCollection services,
        string name, Npgsql.NpgsqlDataSource dataSource, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new PostgreSqlOrderedDispatchOptions();
        return services.AddRelayerPostgreSqlOrderedDispatch(name, dataSource, new PostgreSqlOrderedDispatchOptions
        {
            Schema = configuration[nameof(defaults.Schema)],
            TablePrefix = configuration[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            Lanes = ReadOrdered(configuration, nameof(defaults.Lanes), defaults.Lanes),
            LeaseDuration = ReadOrdered(configuration, nameof(defaults.LeaseDuration), defaults.LeaseDuration),
            PollInterval = ReadOrdered(configuration, nameof(defaults.PollInterval), defaults.PollInterval),
            RetainSentMessages = ReadOrdered(configuration, nameof(defaults.RetainSentMessages), defaults.RetainSentMessages),
            MaxDispatchAttempts = ReadOrdered(configuration, nameof(defaults.MaxDispatchAttempts), defaults.MaxDispatchAttempts),
            BatchSize = ReadOrdered(configuration, nameof(defaults.BatchSize), defaults.BatchSize)
        });
    }

    private static T ReadOrdered<T>(IConfiguration configuration, string key, T fallback) where T : IParsable<T>
    {
        var value = configuration[key];
        if (value is null) return fallback;
        if (T.TryParse(value, CultureInfo.InvariantCulture, out var result)) return result;
        throw new ArgumentException($"Ordered dispatch {key}: '{value}' is invalid.", nameof(configuration));
    }

    private sealed record OrderedRegistration(string Name, PostgreSqlOrderedDispatchOptions Options);

    private sealed class OrderedOptionsFactory(IEnumerable<OrderedRegistration> registrations,
        IEnumerable<IConfigureOptions<PostgreSqlOrderedDispatchOptions>> setups,
        IEnumerable<IPostConfigureOptions<PostgreSqlOrderedDispatchOptions>> postConfigures,
        IEnumerable<IValidateOptions<PostgreSqlOrderedDispatchOptions>> validations)
        : OptionsFactory<PostgreSqlOrderedDispatchOptions>(setups, postConfigures, validations)
    {
        protected override PostgreSqlOrderedDispatchOptions CreateInstance(string requestedName)
        {
            foreach (var registration in registrations)
                if (registration.Name == requestedName) return registration.Options;
            return new();
        }
    }

}
