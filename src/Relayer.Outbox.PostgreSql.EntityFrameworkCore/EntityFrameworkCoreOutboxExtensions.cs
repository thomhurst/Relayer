using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Registers scoped EF Core transaction enlistment. Dispatch is configured separately.</summary>
public static class EntityFrameworkCoreOutboxExtensions
{
    /// <summary>Adds the scoped send enlistment shared by all participating DbContexts.</summary>
    public static IServiceCollection AddRelayerEntityFrameworkCoreOutbox(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => !descriptor.IsKeyedService && descriptor.ServiceType == typeof(EntityFrameworkCoreOutbox)))
        {
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        }

        services.TryAddSingleton<RootProvider>(static provider => new(provider));
        services.TryAddSingleton<DefaultOutboxCapabilities>();
        services.TryAddScoped<EntityFrameworkCoreOutbox>(static provider =>
        {
            var capabilities = provider.GetRequiredService<DefaultOutboxCapabilities>().Get();
            return new(capabilities.Scheduling, capabilities.OrderedScheduling, provider.GetService<RelayerInstrumentation>());
        });
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<EntityFrameworkCoreOutbox>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<EntityFrameworkCoreOutboxOptions>, OutboxOptionsValidator>());
        services.AddOptions<EntityFrameworkCoreOutboxOptions>().ValidateOnStart();
        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    public static IServiceCollection AddRelayerEntityFrameworkCoreOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerEntityFrameworkCoreOutbox();
        services.AddSingleton(new OutboxConfigurationSection(configuration));
        services.TryAddEnumerable(ServiceDescriptor.Transient<IOptionsFactory<EntityFrameworkCoreOutboxOptions>, OutboxOptionsFactory>());
        services.AddSingleton<IOptionsChangeTokenSource<EntityFrameworkCoreOutboxOptions>>(
            new ConfigurationChangeTokenSource<EntityFrameworkCoreOutboxOptions>(configuration));
        return services;
    }

    /// <summary>Installs interceptors on a non-pooled PostgreSQL DbContext. Use the same scoped provider as ISender.</summary>
    public static TBuilder AddRelayerOutbox<TBuilder>(this TBuilder builder,
        IServiceProvider services, EntityFrameworkCoreOutboxOptions? options = null)
        where TBuilder : DbContextOptionsBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(services);
        OutboxConfiguration.ValidatePooling(builder.Options);
        // Pooled options callbacks run at the root before EF adds MaxPoolSize. Reject capturing
        // that provider even when DI scope validation is disabled or EF's provider cache is warm.
        if (ReferenceEquals(services.GetRequiredService<IServiceProvider>(),
                services.GetRequiredService<RootProvider>().Provider))
        {
            throw new InvalidOperationException("Relayer EF Core interceptors must bind to the current ISender scope, " +
                "not the root service provider. Use AddDbContext instead of AddDbContextPool or AddPooledDbContextFactory " +
                "and configure AddRelayerOutbox with its scoped provider.");
        }

        if (builder.Options.HasRelayerOutbox())
        {
            throw new InvalidOperationException("AddRelayerOutbox is already configured on these DbContext options.");
        }

        options ??= services.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value;

        if (services.GetService<TenantContext>() is { } tenant)
        {
            if (options.TenantBinding is null)
            {
                throw new InvalidOperationException("Configure TenantBinding on AddRelayerOutbox in the authorized native DbContext factory when using AddRelayerTenancy.");
            }

            options.TenantBinding.ValidateContext(tenant);
        }

        var outbox = services.GetRequiredService<EntityFrameworkCoreOutbox>();
        PostgreSqlDirectDispatcher? dispatcher = null;
        IEnumerable<PostgreSqlSchedulingRegistration> workers = services.GetServices<PostgreSqlSchedulingRegistration>();

        if (options.DispatchStoreName is { } name)
        {
            dispatcher = services.GetKeyedService<PostgreSqlDirectDispatcher>(name);

            if (dispatcher is null && services.GetService<PostgreSqlTenantDispatcher>() is { } catalog)
            {
                dispatcher = catalog.GetDispatcher(name, out var catalogWorkers);
                workers = catalogWorkers;
            }

            if (dispatcher is null)
            {
                throw new InvalidOperationException($"PostgreSQL dispatch store '{name}' is not registered.");
            }
        }

        var configuration = new PostgreSqlOutboxConfiguration(options, dispatcher, workers, services.GetService<PostgreSqlLocalStore[]>());

        foreach (var extension in builder.Options.Extensions)
        {
            if (extension.Info.IsDatabaseProvider)
            {
                configuration.Validate(builder.Options);
                break;
            }
        }

        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(configuration);
        OutboxSaveChangesInterceptorAggregator.Configure(builder);
        OutboxTransactionInterceptorAggregator.Configure(builder);
        builder.AddInterceptors(new OutboxSaveChangesInterceptor(outbox, configuration, services),
            new OutboxTransactionInterceptor(outbox, configuration));
        return builder;
    }

    /// <summary>Returns whether AddRelayerOutbox installed the outbox interceptors on these options.</summary>
    public static bool HasRelayerOutbox(this DbContextOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.FindExtension<CoreOptionsExtension>()?.Interceptors is { } interceptors)
        {
            foreach (var interceptor in interceptors)
            {
                if (interceptor is OutboxSaveChangesInterceptor)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>Selects the context that owns sends. Retained for callers compiled against the disposable return type.</summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    public static IDisposable EnlistRelayerOutbox(DbContext context)
    {
        return DbContextOutboxExtensions.EnlistRelayerOutbox(context);
    }

    private sealed class RootProvider(IServiceProvider provider)
    {
        public IServiceProvider Provider
        {
            get;
        } = provider;
    }

    private sealed class OutboxOptionsValidator : IValidateOptions<EntityFrameworkCoreOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, EntityFrameworkCoreOutboxOptions options)
        {
            try
            {
                _ = new PostgreSqlOutboxConfiguration(options);
                return ValidateOptionsResult.Success;
            }
            catch (ArgumentException error)
            {
                return ValidateOptionsResult.Fail(error.Message);
            }
        }
    }

    private sealed record OutboxConfigurationSection(IConfiguration Configuration);

    // The configuration binding generator skips init-only setters; bind scalars without runtime reflection.
    private sealed class OutboxOptionsFactory(IEnumerable<OutboxConfigurationSection> sections,
        IEnumerable<IConfigureOptions<EntityFrameworkCoreOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<EntityFrameworkCoreOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<EntityFrameworkCoreOutboxOptions>> validations)
        : OptionsFactory<EntityFrameworkCoreOutboxOptions>(setups, postConfigures, validations)
    {
        protected override EntityFrameworkCoreOutboxOptions CreateInstance(string name)
        {
            var options = new EntityFrameworkCoreOutboxOptions();

            foreach (var section in sections)
            {
                var configuration = section.Configuration;
                options = new()
                {
                    Schema = configuration[nameof(options.Schema)] ?? options.Schema,
                    TablePrefix = configuration[nameof(options.TablePrefix)] ?? options.TablePrefix,
                    Lanes = Read(configuration, name, nameof(options.Lanes), options.Lanes),
                    DispatchStoreName = configuration[nameof(options.DispatchStoreName)] ?? options.DispatchStoreName,
                    LeaseDuration = Read(configuration, name, nameof(options.LeaseDuration), options.LeaseDuration),
                    MaxDispatchAttempts = Read(configuration, name, nameof(options.MaxDispatchAttempts), options.MaxDispatchAttempts)
                };
            }

            return options;
        }

        private static T Read<T>(IConfiguration configuration, string name, string key, T fallback) where T : IParsable<T>
        {
            var value = configuration[key];

            if (value is null)
            {
                return fallback;
            }

            if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }

            throw new OptionsValidationException(name, typeof(EntityFrameworkCoreOutboxOptions),
                [$"{key}: '{value}' is invalid."]);
        }
    }
}
