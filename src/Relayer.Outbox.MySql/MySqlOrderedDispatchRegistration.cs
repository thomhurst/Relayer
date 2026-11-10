using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlConnector;
using Relayer.Transports;

namespace Relayer.Outbox.MySql;

/// <summary>Explicit registration of named MySQL ordered dispatch workers.</summary>
public static partial class MySqlOutboxServiceCollectionExtensions
{
    /// <summary>Starts one named worker for ordered rows. Deploy schema and register native transports first.</summary>
    /// <remarks>The application owns the data source. Writer and all workers must use identical schema, prefix and lane count.</remarks>
    public static IServiceCollection AddRelayerMySqlOrderedDispatch(this IServiceCollection services,
        string name, MySqlDataSource dataSource, MySqlOrderedDispatchOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(dataSource);
        MySqlOutboxStore.ValidateDataSource(dataSource);
        options ??= new();
        _ = options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(MySqlOrderedDispatcher) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"Ordered outbox dispatcher '{name}' is already registered.");
        var snapshot = options;
        services.AddOptions<MySqlOrderedDispatchOptions>(name);
        services.AddSingleton(new OrderedRegistration(name, snapshot));
        services.TryAddSingleton<IOptionsFactory<MySqlOrderedDispatchOptions>>(provider =>
            new OrderedOptionsFactory(provider.GetServices<OrderedRegistration>(), provider.GetServices<IConfigureOptions<MySqlOrderedDispatchOptions>>(),
                provider.GetServices<IPostConfigureOptions<MySqlOrderedDispatchOptions>>(),
                provider.GetServices<IValidateOptions<MySqlOrderedDispatchOptions>>()));
        services.AddKeyedSingleton<MySqlOrderedDispatcher>(name, (provider, _) =>
            new(dataSource, provider.GetRequiredService<IOptionsMonitor<MySqlOrderedDispatchOptions>>().Get(name),
                provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<MySqlOrderedDispatcher>>(),
                instrumentation: provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<MySqlOrderedDispatcher>(name));
        return services;
    }

    /// <summary>Binds immutable ordered-dispatch settings without reflection, including under Native AOT.</summary>
    public static IServiceCollection AddRelayerMySqlOrderedDispatch(this IServiceCollection services,
        string name, MySqlDataSource dataSource, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new MySqlOrderedDispatchOptions();
        return services.AddRelayerMySqlOrderedDispatch(name, dataSource, new MySqlOrderedDispatchOptions
        {
            TablePrefix = configuration[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            Lanes = ReadOrdered(configuration, nameof(defaults.Lanes), defaults.Lanes),
            LeaseDuration = ReadOrdered(configuration, nameof(defaults.LeaseDuration), defaults.LeaseDuration),
            PollInterval = ReadOrdered(configuration, nameof(defaults.PollInterval), defaults.PollInterval),
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

    private sealed record OrderedRegistration(string Name, MySqlOrderedDispatchOptions Options);

    private sealed class OrderedOptionsFactory(IEnumerable<OrderedRegistration> registrations,
        IEnumerable<IConfigureOptions<MySqlOrderedDispatchOptions>> setups,
        IEnumerable<IPostConfigureOptions<MySqlOrderedDispatchOptions>> postConfigures,
        IEnumerable<IValidateOptions<MySqlOrderedDispatchOptions>> validations)
        : OptionsFactory<MySqlOrderedDispatchOptions>(setups, postConfigures, validations)
    {
        protected override MySqlOrderedDispatchOptions CreateInstance(string requestedName)
        {
            foreach (var registration in registrations)
                if (registration.Name == requestedName) return registration.Options;
            return new();
        }
    }

}
