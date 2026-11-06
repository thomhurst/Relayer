using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Relayer.Outbox.MySql;

/// <summary>Registers scoped MySQL transaction enlistment. Stores and dispatch are configured separately.</summary>
public static class MySqlOutboxRegistration
{
    /// <summary>Adds the MySQL outbox and the scoped IOutboxEnlistment used by ISender.</summary>
    public static IServiceCollection AddRelayerMySqlOutbox(this IServiceCollection services, MySqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(static descriptor => descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => descriptor.ServiceType == typeof(MySqlOutbox)))
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        if (options is not null) MySqlOutbox.ValidateOptions(options);
        services.TryAddScoped<MySqlOutbox>(provider =>
        {
            var snapshot = provider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>();
            var dispatcher = snapshot.Value.DispatchStoreName is { } name
                ? provider.GetRequiredKeyedService<MySqlDirectDispatcher>(name) : null;
            return new(snapshot, provider.GetService<RelayerInstrumentation>(), dispatcher);
        });
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<MySqlOutbox>());
        services.TryAddSingleton<IValidateOptions<MySqlOutboxOptions>, Validator>();
        services.AddOptions<MySqlOutboxOptions>().ValidateOnStart();
        if (options is not null) AddFactory(services, options, null);
        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    public static IServiceCollection AddRelayerMySqlOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerMySqlOutbox();
        AddFactory(services, new(), configuration);
        services.AddSingleton<IOptionsChangeTokenSource<MySqlOutboxOptions>>(
            new ConfigurationChangeTokenSource<MySqlOutboxOptions>(configuration));
        return services;
    }

    private static void AddFactory(IServiceCollection services, MySqlOutboxOptions options, IConfiguration? configuration) =>
        services.AddSingleton<IOptionsFactory<MySqlOutboxOptions>>(provider => new Factory(options, configuration,
            provider.GetServices<IConfigureOptions<MySqlOutboxOptions>>(),
            provider.GetServices<IPostConfigureOptions<MySqlOutboxOptions>>(),
            provider.GetServices<IValidateOptions<MySqlOutboxOptions>>()));

    private sealed class Validator : IValidateOptions<MySqlOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, MySqlOutboxOptions options)
        {
            try { MySqlOutbox.ValidateOptions(options); return ValidateOptionsResult.Success; }
            catch (ArgumentException error) { return ValidateOptionsResult.Fail(error.Message); }
        }
    }

    // Configuration's source generator skips init-only setters. Bind scalars without runtime reflection.
    private sealed class Factory(MySqlOutboxOptions defaults, IConfiguration? configuration,
        IEnumerable<IConfigureOptions<MySqlOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<MySqlOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<MySqlOutboxOptions>> validations)
        : OptionsFactory<MySqlOutboxOptions>(setups, postConfigures, validations)
    {
        protected override MySqlOutboxOptions CreateInstance(string name) => new()
        {
            TablePrefix = configuration?[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
            DispatchStoreName = configuration?[nameof(defaults.DispatchStoreName)] ?? defaults.DispatchStoreName,
            Lanes = Read(nameof(defaults.Lanes), defaults.Lanes)
        };

        private T Read<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration?[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed)) return parsed;
            throw new OptionsValidationException(Options.DefaultName, typeof(MySqlOutboxOptions), [$"{key}: '{value}' is invalid."]);
        }
    }
}
