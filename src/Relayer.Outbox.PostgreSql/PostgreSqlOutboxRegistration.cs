using Relayer.Transports;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Registers scoped PostgreSQL transaction enlistment. Stores and dispatch are configured separately.</summary>
public static class PostgreSqlOutboxRegistration
{
    /// <summary>Adds the PostgreSQL outbox and the scoped IOutboxEnlistment used by ISender.</summary>
    public static IServiceCollection AddRelayerPostgreSqlOutbox(this IServiceCollection services, PostgreSqlOutboxOptions? options = null)
        => Register(services, options, null);

    /// <summary>Adds transactional enqueue with immediate commit-triggered dispatch through a named shared worker.</summary>
    public static IServiceCollection AddRelayerPostgreSqlOutbox(this IServiceCollection services, string dispatchStoreName,
        PostgreSqlOutboxOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dispatchStoreName);
        return Register(services, options, dispatchStoreName);
    }

    private static IServiceCollection Register(IServiceCollection services, PostgreSqlOutboxOptions? options, string? dispatchStoreName)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(static descriptor => descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => descriptor.ServiceType == typeof(PostgreSqlOutbox)))
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        if (options is not null) PostgreSqlOutbox.ValidateOptions(options);
        services.TryAddScoped<PostgreSqlOutbox>(provider =>
        {
            var snapshot = provider.GetRequiredService<IOptionsSnapshot<PostgreSqlOutboxOptions>>();
            if (provider.GetService<TenantContext>() is { } tenant)
            {
                if (snapshot.Value.TenantBinding is null)
                    throw new InvalidOperationException("Configure TenantBinding in the authorized native store factory when using AddRelayerTenancy with the PostgreSQL outbox.");
                snapshot.Value.TenantBinding.ValidateContext(tenant);
            }
            return new(snapshot, provider.GetServices<PostgreSqlSchedulingRegistration>(), provider.GetService<RelayerInstrumentation>(),
                dispatchStoreName is null ? null : provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>(dispatchStoreName));
        });
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<PostgreSqlOutbox>());
        services.TryAddSingleton<IValidateOptions<PostgreSqlOutboxOptions>, Validator>();
        services.AddOptions<PostgreSqlOutboxOptions>().ValidateOnStart();
        if (options is not null) AddFactory(services, options, null);
        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    public static IServiceCollection AddRelayerPostgreSqlOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerPostgreSqlOutbox();
        AddFactory(services, new(), configuration);
        services.AddSingleton<IOptionsChangeTokenSource<PostgreSqlOutboxOptions>>(
            new ConfigurationChangeTokenSource<PostgreSqlOutboxOptions>(configuration));
        return services;
    }

    private static void AddFactory(IServiceCollection services, PostgreSqlOutboxOptions options, IConfiguration? configuration) =>
        services.AddSingleton<IOptionsFactory<PostgreSqlOutboxOptions>>(provider => new Factory(options, configuration,
            provider.GetServices<IConfigureOptions<PostgreSqlOutboxOptions>>(),
            provider.GetServices<IPostConfigureOptions<PostgreSqlOutboxOptions>>(),
            provider.GetServices<IValidateOptions<PostgreSqlOutboxOptions>>()));

    private sealed class Validator : IValidateOptions<PostgreSqlOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, PostgreSqlOutboxOptions options)
        {
            try { PostgreSqlOutbox.ValidateOptions(options); return ValidateOptionsResult.Success; }
            catch (ArgumentException error) { return ValidateOptionsResult.Fail(error.Message); }
        }
    }

    // Configuration's source generator skips init-only setters. Bind scalars without runtime reflection.
    private sealed class Factory(PostgreSqlOutboxOptions defaults, IConfiguration? configuration,
        IEnumerable<IConfigureOptions<PostgreSqlOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<PostgreSqlOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<PostgreSqlOutboxOptions>> validations)
        : OptionsFactory<PostgreSqlOutboxOptions>(setups, postConfigures, validations)
    {
        protected override PostgreSqlOutboxOptions CreateInstance(string name) => new()
        {
            TenantBinding = defaults.TenantBinding,
            Schema = configuration?[nameof(defaults.Schema)] ?? defaults.Schema,
            TablePrefix = configuration?[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            Lanes = Read(nameof(defaults.Lanes), defaults.Lanes),
            LeaseDuration = Read(nameof(defaults.LeaseDuration), defaults.LeaseDuration),
            RetainSentMessages = Read(nameof(defaults.RetainSentMessages), defaults.RetainSentMessages),
            MaxDispatchAttempts = Read(nameof(defaults.MaxDispatchAttempts), defaults.MaxDispatchAttempts)
        };

        private T Read<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration?[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed)) return parsed;
            throw new OptionsValidationException(Options.DefaultName, typeof(PostgreSqlOutboxOptions), [$"{key}: '{value}' is invalid."]);
        }
    }
}
