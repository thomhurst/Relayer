using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer;

/// <summary>Explicit registrations for SQL Server ordered dispatch.</summary>
public static partial class SqlServerOutboxServiceCollectionExtensions
{
    /// <summary>Starts one named worker for ordered rows. Deploy schema and register native transports first.</summary>
    /// <remarks>Native connection settings pass through unchanged. Writer and all workers must use identical database, schema, prefix and lane count.</remarks>
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("SqlClient runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("SqlClient runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerOrderedDispatch(this IServiceCollection services,
        string name, string connectionString, SqlServerOrderedDispatchOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
        options ??= new();
        _ = options.Validate();
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(SqlServerOrderedDispatcher) && Equals(descriptor.ServiceKey, name))
                throw new InvalidOperationException($"Ordered outbox dispatcher '{name}' is already registered.");
        var snapshot = options;
        services.AddOptions<SqlServerOrderedDispatchOptions>(name);
        services.AddSingleton(new OrderedRegistration(name, snapshot));
        services.TryAddSingleton<IOptionsFactory<SqlServerOrderedDispatchOptions>>(provider =>
            new OrderedOptionsFactory(provider.GetServices<OrderedRegistration>(), provider.GetServices<IConfigureOptions<SqlServerOrderedDispatchOptions>>(),
                provider.GetServices<IPostConfigureOptions<SqlServerOrderedDispatchOptions>>(),
                provider.GetServices<IValidateOptions<SqlServerOrderedDispatchOptions>>()));
        services.AddKeyedSingleton<SqlServerOrderedDispatcher>(name, (provider, _) =>
            new(connectionString, provider.GetRequiredService<IOptionsMonitor<SqlServerOrderedDispatchOptions>>().Get(name),
                provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<SqlServerOrderedDispatcher>>(),
                instrumentation: provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<SqlServerOrderedDispatcher>(name));
        return services;
    }

    /// <summary>Binds immutable ordered-dispatch settings without reflection. SqlClient runtime trimming/AOT restrictions still apply.</summary>
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("SqlClient runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("SqlClient runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerOrderedDispatch(this IServiceCollection services,
        string name, string connectionString, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new SqlServerOrderedDispatchOptions();
        return services.AddRelayerSqlServerOrderedDispatch(name, connectionString, new SqlServerOrderedDispatchOptions
        {
            Schema = configuration[nameof(defaults.Schema)],
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

    private sealed record OrderedRegistration(string Name, SqlServerOrderedDispatchOptions Options);

    private sealed class OrderedOptionsFactory(IEnumerable<OrderedRegistration> registrations,
        IEnumerable<IConfigureOptions<SqlServerOrderedDispatchOptions>> setups,
        IEnumerable<IPostConfigureOptions<SqlServerOrderedDispatchOptions>> postConfigures,
        IEnumerable<IValidateOptions<SqlServerOrderedDispatchOptions>> validations)
        : OptionsFactory<SqlServerOrderedDispatchOptions>(setups, postConfigures, validations)
    {
        protected override SqlServerOrderedDispatchOptions CreateInstance(string requestedName)
        {
            foreach (var registration in registrations)
                if (registration.Name == requestedName) return registration.Options;
            return new();
        }
    }

}
