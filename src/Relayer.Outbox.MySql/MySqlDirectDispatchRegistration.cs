using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Outbox.MySql;

/// <summary>Registers explicitly named MySQL durable read/claim stores.</summary>
public static partial class MySqlOutboxServiceCollectionExtensions
{
    /// <summary>Registers a named durable store and a hosted worker for unordered, commit-triggered sends.</summary>
    /// <remarks>
    /// Select this name with MySqlOutboxOptions.DispatchStoreName on participating scopes.
    /// The application owns the data source; its native connection string must specify Database.
    /// Writers must use the same database spelling. This does not dispatch ordered or future-due rows or run recovery polling.
    /// </remarks>
    public static IServiceCollection AddRelayerMySqlDirectDispatch(this IServiceCollection services,
        string storeName, MySqlConnector.MySqlDataSource dataSource, MySqlOutboxOptions? storeOptions = null,
        MySqlDirectDispatchOptions? dispatchOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentNullException.ThrowIfNull(dataSource);
        _ = MySqlDirectDispatcher.ValidateDataSource(dataSource);
        if (services.Any(descriptor => descriptor.ServiceType == typeof(MySqlDirectDispatcher) && Equals(descriptor.ServiceKey, storeName)))
            throw new InvalidOperationException($"Direct outbox dispatch store '{storeName}' is already registered.");
        dispatchOptions ??= new();
        if (dispatchOptions.QueueCapacity is <= 0 or int.MaxValue || dispatchOptions.BatchSize <= 0 || dispatchOptions.ClaimDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(dispatchOptions), "QueueCapacity and BatchSize must be positive, QueueCapacity must be below Int32.MaxValue, and ClaimDuration must be positive.");
        storeOptions ??= new();
        var snapshot = new MySqlOutboxOptions { TablePrefix = storeOptions.TablePrefix, Lanes = storeOptions.Lanes };
        _ = MySqlOutboxSchema.CreateScript(snapshot);
        services.AddRelayerMySqlOutboxStore(storeName, dataSource, snapshot);
        services.AddSingleton(new DispatchSettings(storeName, dispatchOptions));
        services.AddOptions<MySqlDirectDispatchOptions>();
        services.TryAddSingleton<IOptionsFactory<MySqlDirectDispatchOptions>, DispatchOptionsFactory>();
        services.AddKeyedSingleton<MySqlDirectDispatcher>(storeName, (provider, _) =>
            new MySqlDirectDispatcher(dataSource, snapshot,
                provider.GetRequiredService<IOptionsMonitor<MySqlDirectDispatchOptions>>().Get(storeName),
                provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<MySqlDirectDispatcher>>(),
                instrumentation: provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<MySqlDirectDispatcher>(storeName));
        return services;
    }

    /// <summary>Binds immutable direct-dispatch limits from configuration at registration.</summary>
    public static IServiceCollection AddRelayerMySqlDirectDispatch(this IServiceCollection services,
        string storeName, MySqlConnector.MySqlDataSource dataSource, IConfiguration configuration,
        MySqlOutboxOptions? storeOptions = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new MySqlDirectDispatchOptions();
        return services.AddRelayerMySqlDirectDispatch(storeName, dataSource, storeOptions, new MySqlDirectDispatchOptions
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
        throw new OptionsValidationException(Options.DefaultName, typeof(MySqlDirectDispatchOptions), [$"{key}: '{value}' is invalid."]);
    }

    private sealed record DispatchSettings(string Name, MySqlDirectDispatchOptions Options);

    private sealed class DispatchOptionsFactory(IEnumerable<DispatchSettings> settings,
        IEnumerable<IConfigureOptions<MySqlDirectDispatchOptions>> setups,
        IEnumerable<IPostConfigureOptions<MySqlDirectDispatchOptions>> postConfigures,
        IEnumerable<IValidateOptions<MySqlDirectDispatchOptions>> validations)
        : OptionsFactory<MySqlDirectDispatchOptions>(setups, postConfigures, validations)
    {
        protected override MySqlDirectDispatchOptions CreateInstance(string name)
        {
            foreach (var setting in settings)
                if (setting.Name == name) return setting.Options;
            return new();
        }
    }

}
