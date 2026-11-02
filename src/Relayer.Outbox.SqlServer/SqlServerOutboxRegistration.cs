using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Relayer.Outbox.SqlServer;

/// <summary>Registers scoped SQL Server transaction enlistment. Stores and dispatch are configured separately.</summary>
public static class SqlServerOutboxRegistration
{
    /// <summary>Adds the SQL Server outbox and the scoped IOutboxEnlistment used by ISender.</summary>
    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerOutbox(this IServiceCollection services, SqlServerOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(static descriptor => descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => descriptor.ServiceType == typeof(SqlServerOutbox)))
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        if (options is not null) SqlServerOutbox.ValidateOptions(options);
        services.TryAddScoped<SqlServerOutbox>(provider =>
        {
            var snapshot = provider.GetRequiredService<IOptionsSnapshot<SqlServerOutboxOptions>>();
            var dispatcher = snapshot.Value.DispatchStoreName is { } name
                ? provider.GetRequiredKeyedService<SqlServerDirectDispatcher>(name) : null;
            return new(snapshot, provider.GetService<RelayerInstrumentation>(), dispatcher);
        });
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<SqlServerOutbox>());
        services.TryAddSingleton<IValidateOptions<SqlServerOutboxOptions>, Validator>();
        services.AddOptions<SqlServerOutboxOptions>().ValidateOnStart();
        if (options is not null) AddFactory(services, options, null);
        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static IServiceCollection AddRelayerSqlServerOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerSqlServerOutbox();
        AddFactory(services, new(), configuration);
        services.AddSingleton<IOptionsChangeTokenSource<SqlServerOutboxOptions>>(
            new ConfigurationChangeTokenSource<SqlServerOutboxOptions>(configuration));
        return services;
    }

    private static void AddFactory(IServiceCollection services, SqlServerOutboxOptions options, IConfiguration? configuration) =>
        services.AddSingleton<IOptionsFactory<SqlServerOutboxOptions>>(provider => new Factory(options, configuration,
            provider.GetServices<IConfigureOptions<SqlServerOutboxOptions>>(),
            provider.GetServices<IPostConfigureOptions<SqlServerOutboxOptions>>(),
            provider.GetServices<IValidateOptions<SqlServerOutboxOptions>>()));

    private sealed class Validator : IValidateOptions<SqlServerOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, SqlServerOutboxOptions options)
        {
            try { SqlServerOutbox.ValidateOptions(options); return ValidateOptionsResult.Success; }
            catch (ArgumentException error) { return ValidateOptionsResult.Fail(error.Message); }
        }
    }

    // Configuration's source generator skips init-only setters. Bind scalars without runtime reflection.
    private sealed class Factory(SqlServerOutboxOptions defaults, IConfiguration? configuration,
        IEnumerable<IConfigureOptions<SqlServerOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<SqlServerOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<SqlServerOutboxOptions>> validations)
        : OptionsFactory<SqlServerOutboxOptions>(setups, postConfigures, validations)
    {
        protected override SqlServerOutboxOptions CreateInstance(string name) => new()
        {
            DispatchStoreName = configuration?[nameof(defaults.DispatchStoreName)] ?? defaults.DispatchStoreName,
            Schema = configuration?[nameof(defaults.Schema)] ?? defaults.Schema,
            TablePrefix = configuration?[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            Lanes = Read(nameof(defaults.Lanes), defaults.Lanes)
        };

        private T Read<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration?[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed)) return parsed;
            throw new OptionsValidationException(Options.DefaultName, typeof(SqlServerOutboxOptions), [$"{key}: '{value}' is invalid."]);
        }
    }
}
