using System.Globalization;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer;

/// <summary>Registers explicitly named SQL Server durable read/claim stores.</summary>
public static partial class SqlServerOutboxServiceCollectionExtensions
{
    /// <summary>Registers a named durable store and a hosted worker for unordered, commit-triggered sends.</summary>
    /// <remarks>
    /// Select this name with SqlServerOutboxOptions.DispatchStoreName on participating scopes.
    /// The native connection string passes through unchanged and must specify Initial Catalog (Database).
    /// Writers must use the same database spelling. This does not dispatch ordered or future-due rows or run recovery polling.
    /// </remarks>
    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerDirectDispatch(this IServiceCollection services,
        string storeName, string connectionString, SqlServerOutboxOptions? storeOptions = null,
        SqlServerDirectDispatchOptions? dispatchOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(storeName);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = SqlServerDirectDispatcher.ValidateConnectionString(connectionString);
        if (services.Any(descriptor => descriptor.ServiceType == typeof(SqlServerDirectDispatcher) && Equals(descriptor.ServiceKey, storeName)))
            throw new InvalidOperationException($"Direct outbox dispatch store '{storeName}' is already registered.");
        dispatchOptions ??= new();
        if (dispatchOptions.QueueCapacity is <= 0 or int.MaxValue || dispatchOptions.BatchSize <= 0 || dispatchOptions.ClaimDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(dispatchOptions), "QueueCapacity and BatchSize must be positive, QueueCapacity must be below Int32.MaxValue, and ClaimDuration must be positive.");
        storeOptions ??= new();
        var snapshot = new SqlServerOutboxOptions { Schema = storeOptions.Schema, TablePrefix = storeOptions.TablePrefix, Lanes = storeOptions.Lanes, MaxDispatchAttempts = storeOptions.MaxDispatchAttempts, RetrySendFailure = storeOptions.RetrySendFailure };
        _ = SqlServerOutboxSchema.CreateScript(snapshot);
        services.AddRelayerSqlServerOutboxStore(storeName, connectionString, snapshot);
        services.AddSingleton(new DispatchSettings(storeName, dispatchOptions));
        services.AddOptions<SqlServerDirectDispatchOptions>();
        services.TryAddSingleton<IOptionsFactory<SqlServerDirectDispatchOptions>, DispatchOptionsFactory>();
        services.AddKeyedSingleton<SqlServerDirectDispatcher>(storeName, (provider, _) =>
            new SqlServerDirectDispatcher(connectionString, snapshot,
                provider.GetRequiredService<IOptionsMonitor<SqlServerDirectDispatchOptions>>().Get(storeName),
                provider.GetServices<ITransport>(), provider.GetRequiredService<ILogger<SqlServerDirectDispatcher>>(),
                instrumentation: provider.GetService<RelayerInstrumentation>()));
        services.AddSingleton<IHostedService>(provider => provider.GetRequiredKeyedService<SqlServerDirectDispatcher>(storeName));
        return services;
    }

    /// <summary>Binds immutable direct-dispatch limits from configuration at registration.</summary>
    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerDirectDispatch(this IServiceCollection services,
        string storeName, string connectionString, IConfiguration configuration,
        SqlServerOutboxOptions? storeOptions = null)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var defaults = new SqlServerDirectDispatchOptions();
        return services.AddRelayerSqlServerDirectDispatch(storeName, connectionString, storeOptions, new SqlServerDirectDispatchOptions
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
        throw new OptionsValidationException(Options.DefaultName, typeof(SqlServerDirectDispatchOptions), [$"{key}: '{value}' is invalid."]);
    }

    private sealed record DispatchSettings(string Name, SqlServerDirectDispatchOptions Options);

    private sealed class DispatchOptionsFactory(IEnumerable<DispatchSettings> settings,
        IEnumerable<IConfigureOptions<SqlServerDirectDispatchOptions>> setups,
        IEnumerable<IPostConfigureOptions<SqlServerDirectDispatchOptions>> postConfigures,
        IEnumerable<IValidateOptions<SqlServerDirectDispatchOptions>> validations)
        : OptionsFactory<SqlServerDirectDispatchOptions>(setups, postConfigures, validations)
    {
        protected override SqlServerDirectDispatchOptions CreateInstance(string name)
        {
            foreach (var setting in settings)
                if (setting.Name == name) return setting.Options;
            return new();
        }
    }

}
