using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Relayer.Outbox;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

internal sealed record PostgreSqlOutboxWriterRegistration(string? DispatchStoreName);

/// <summary>Registers scoped PostgreSQL transaction enlistment. Stores and dispatch are configured separately.</summary>
public static class PostgreSqlOutboxRegistration
{
    /// <summary>Adds the PostgreSQL outbox and the scoped IOutboxEnlistment used by ISender.</summary>
    public static IServiceCollection AddRelayerPostgreSqlOutbox(this IServiceCollection services, PostgreSqlOutboxOptions? options = null)
    {
        return Register(services, options, null);
    }

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

        if (services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(PostgreSqlOutbox)))
        {
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        }

        if (options is not null)
        {
            PostgreSqlOutbox.ValidateOptions(options);
        }

        services.TryAddScoped<PostgreSqlOutbox>(provider =>
        {
            var snapshot = provider.GetRequiredService<IOptionsSnapshot<PostgreSqlOutboxOptions>>();

            if (provider.GetService<TenantContext>() is { } tenant)
            {
                if (snapshot.Value.TenantBinding is null)
                {
                    throw new InvalidOperationException("Configure TenantBinding in the authorized native store factory when using AddRelayerTenancy with the PostgreSQL outbox.");
                }

                snapshot.Value.TenantBinding.ValidateContext(tenant);
            }

            return new(snapshot, provider.GetServices<PostgreSqlSchedulingRegistration>(), provider.GetService<RelayerInstrumentation>(),
                dispatchStoreName is null ? null : provider.GetRequiredKeyedService<PostgreSqlDirectDispatcher>(dispatchStoreName),
                provider.GetService<PostgreSqlLocalStore[]>());
        });
        services.TryAddSingleton(new PostgreSqlOutboxWriterRegistration(dispatchStoreName));
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<PostgreSqlOutbox>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<PostgreSqlOutboxOptions>, Validator>());
        services.AddOptions<PostgreSqlOutboxOptions>().ValidateOnStart();

        if (options is not null)
        {
            AddFactory(services, options, null);
        }

        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    public static IServiceCollection AddRelayerPostgreSqlOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerPostgreSqlOutbox();
        AddFactory(services, new(), configuration);
        return services;
    }

    private static void AddFactory(IServiceCollection services, PostgreSqlOutboxOptions options, IConfiguration? configuration)
    {
        var sections = OutboxOptionsConfiguration<PostgreSqlOutboxOptions>.Register(services, configuration);
        services.Add(new ServiceDescriptor(typeof(IOptionsFactory<PostgreSqlOutboxOptions>),
            provider => new Factory(options, sections,
                provider.GetServices<IConfigureOptions<PostgreSqlOutboxOptions>>(),
                provider.GetServices<IPostConfigureOptions<PostgreSqlOutboxOptions>>(),
                provider.GetServices<IValidateOptions<PostgreSqlOutboxOptions>>()),
            configuration is null ? ServiceLifetime.Singleton : ServiceLifetime.Transient));
    }

    private sealed class Validator : IValidateOptions<PostgreSqlOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, PostgreSqlOutboxOptions options)
        {
            try
            {
                PostgreSqlOutbox.ValidateOptions(options);
                return ValidateOptionsResult.Success;
            }
            catch (ArgumentException error)
            {
                return ValidateOptionsResult.Fail(error.Message);
            }
        }
    }

    // Configuration's source generator skips init-only setters. Bind scalars without runtime reflection.
    private sealed class Factory(PostgreSqlOutboxOptions defaults, OutboxOptionsConfiguration<PostgreSqlOutboxOptions>? configuration,
        IEnumerable<IConfigureOptions<PostgreSqlOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<PostgreSqlOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<PostgreSqlOutboxOptions>> validations)
        : OptionsFactory<PostgreSqlOutboxOptions>(setups, postConfigures, validations)
    {
        protected override PostgreSqlOutboxOptions CreateInstance(string name)
        {
            return new()
            {
                TenantBinding = defaults.TenantBinding,
                Schema = configuration?[nameof(defaults.Schema)] ?? defaults.Schema,
                TablePrefix = configuration?[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
                Lanes = configuration?.Read<int>(nameof(defaults.Lanes)) ?? defaults.Lanes,
                LeaseDuration = configuration?.Read<TimeSpan>(nameof(defaults.LeaseDuration)) ?? defaults.LeaseDuration,
                RetainSentMessages = configuration?.Read<bool>(nameof(defaults.RetainSentMessages)) ?? defaults.RetainSentMessages,
                MaxDispatchAttempts = configuration?.Read<int>(nameof(defaults.MaxDispatchAttempts)) ?? defaults.MaxDispatchAttempts
            };
        }
    }
}
