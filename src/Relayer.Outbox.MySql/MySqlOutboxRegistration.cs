using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Relayer.Outbox;
using Relayer.Transports;

namespace Relayer.Outbox.MySql;

/// <summary>Registers scoped MySQL transaction enlistment. Stores and dispatch are configured separately.</summary>
public static class MySqlOutboxRegistration
{
    /// <summary>Adds the MySQL outbox and the scoped IOutboxEnlistment used by ISender.</summary>
    public static IServiceCollection AddRelayerMySqlOutbox(this IServiceCollection services, MySqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(MySqlOutbox)))
        {
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        }

        if (options is not null)
        {
            MySqlOutbox.ValidateOptions(options);
        }

        services.TryAddSingleton<MySqlInboxPools>();
        services.TryAddScoped<MySqlOutbox>(provider =>
        {
            var snapshot = provider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>();
            var dispatcher = snapshot.Value.DispatchStoreName is { } name
                ? provider.GetRequiredKeyedService<MySqlDirectDispatcher>(name) : null;
            return new(snapshot, provider.GetService<RelayerInstrumentation>(), dispatcher, provider.GetRequiredService<MySqlInboxPools>());
        });
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<MySqlOutbox>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<MySqlOutboxOptions>, Validator>());
        services.AddOptions<MySqlOutboxOptions>().ValidateOnStart();

        if (options is not null)
        {
            AddFactory(services, options, null);
        }

        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    public static IServiceCollection AddRelayerMySqlOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerMySqlOutbox();
        AddFactory(services, new(), configuration);
        return services;
    }

    private static void AddFactory(IServiceCollection services, MySqlOutboxOptions options, IConfiguration? configuration)
    {
        var sections = OutboxOptionsConfiguration<MySqlOutboxOptions>.Register(services, configuration);
        services.Add(new ServiceDescriptor(typeof(IOptionsFactory<MySqlOutboxOptions>),
            provider => new Factory(options, sections,
                provider.GetServices<IConfigureOptions<MySqlOutboxOptions>>(),
                provider.GetServices<IPostConfigureOptions<MySqlOutboxOptions>>(),
                provider.GetServices<IValidateOptions<MySqlOutboxOptions>>()),
            configuration is null ? ServiceLifetime.Singleton : ServiceLifetime.Transient));
    }

    private sealed class Validator : IValidateOptions<MySqlOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, MySqlOutboxOptions options)
        {
            try
            {
                MySqlOutbox.ValidateOptions(options);
                return ValidateOptionsResult.Success;
            }
            catch (ArgumentException error)
            {
                return ValidateOptionsResult.Fail(error.Message);
            }
        }
    }

    // Configuration's source generator skips init-only setters. Bind scalars without runtime reflection.
    private sealed class Factory(MySqlOutboxOptions defaults, OutboxOptionsConfiguration<MySqlOutboxOptions>? configuration,
        IEnumerable<IConfigureOptions<MySqlOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<MySqlOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<MySqlOutboxOptions>> validations)
        : OptionsFactory<MySqlOutboxOptions>(setups, postConfigures, validations)
    {
        protected override MySqlOutboxOptions CreateInstance(string name)
        {
            return new()
            {
                TablePrefix = configuration?[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
                DispatchStoreName = configuration?[nameof(defaults.DispatchStoreName)] ?? defaults.DispatchStoreName,
                MaxDispatchAttempts = configuration?.Read<int>(nameof(defaults.MaxDispatchAttempts)) ?? defaults.MaxDispatchAttempts,
                Lanes = configuration?.Read<int>(nameof(defaults.Lanes)) ?? defaults.Lanes
            };
        }
    }
}
