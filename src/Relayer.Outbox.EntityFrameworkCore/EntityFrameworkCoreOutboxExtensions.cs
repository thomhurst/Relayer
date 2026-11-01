using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Registers scoped EF Core transaction enlistment. Dispatch is configured separately.</summary>
public static class EntityFrameworkCoreOutboxExtensions
{
    /// <summary>Adds the scoped send enlistment shared by all participating DbContexts.</summary>
    public static IServiceCollection AddRelayerEntityFrameworkCoreOutbox(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(static descriptor => descriptor.ServiceType == typeof(IOutboxEnlistment)) &&
            !services.Any(static descriptor => descriptor.ServiceType == typeof(EntityFrameworkCoreOutbox)))
            throw new InvalidOperationException("A different IOutboxEnlistment is already registered. Configure one scoped outbox integration.");
        services.TryAddScoped<EntityFrameworkCoreOutbox>();
        services.TryAddScoped<IOutboxEnlistment>(static provider => provider.GetRequiredService<EntityFrameworkCoreOutbox>());
        services.TryAddSingleton<IValidateOptions<EntityFrameworkCoreOutboxOptions>, OutboxOptionsValidator>();
        services.AddOptions<EntityFrameworkCoreOutboxOptions>().ValidateOnStart();
        return services;
    }

    /// <summary>Binds immutable options explicitly through the Options pattern, including startup validation and reload for new scopes.</summary>
    public static IServiceCollection AddRelayerEntityFrameworkCoreOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddRelayerEntityFrameworkCoreOutbox();
        services.AddSingleton<IOptionsFactory<EntityFrameworkCoreOutboxOptions>>(provider =>
            new OutboxOptionsFactory(configuration,
                provider.GetServices<IConfigureOptions<EntityFrameworkCoreOutboxOptions>>(),
                provider.GetServices<IPostConfigureOptions<EntityFrameworkCoreOutboxOptions>>(),
                provider.GetServices<IValidateOptions<EntityFrameworkCoreOutboxOptions>>()));
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
        if (builder.Options.FindExtension<CoreOptionsExtension>()?.MaxPoolSize is not null)
            throw new InvalidOperationException("Relayer outbox requires scoped DbContexts. Use AddDbContext instead of AddDbContextPool.");
        if (builder.Options.FindExtension<CoreOptionsExtension>()?.Interceptors is { } interceptors)
            foreach (var interceptor in interceptors)
                if (interceptor is OutboxSaveChangesInterceptor)
                    throw new InvalidOperationException("AddRelayerOutbox is already configured on these DbContext options.");
        options ??= services.GetRequiredService<IOptionsSnapshot<EntityFrameworkCoreOutboxOptions>>().Value;
        var outbox = services.GetRequiredService<EntityFrameworkCoreOutbox>();
        var dispatcher = options.DispatchStoreName is { } name
            ? services.GetRequiredKeyedService<PostgreSqlDirectDispatcher>(name) : null;
        var configuration = new OutboxConfiguration(options, dispatcher);
        builder.AddInterceptors(new OutboxSaveChangesInterceptor(outbox, configuration),
            new OutboxTransactionInterceptor(outbox, configuration));
        return builder;
    }

    /// <summary>Selects the context that owns sends before an implicit SaveChanges transaction, or disambiguates explicit transactions.</summary>
    /// <remarks>Dispose before disposing the context. Uncommitted messages are discarded. Do not use a DI scope concurrently.</remarks>
    public static IDisposable EnlistRelayerOutbox(this DbContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var options = context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>();
        if (options?.Interceptors is { } interceptors)
            foreach (var interceptor in interceptors)
                if (interceptor is OutboxSaveChangesInterceptor save)
                    return save.Outbox.Select(context, save.Configuration);
        throw new InvalidOperationException("Configure this DbContext with AddRelayerOutbox before calling EnlistRelayerOutbox.");
    }

    private sealed class OutboxOptionsValidator : IValidateOptions<EntityFrameworkCoreOutboxOptions>
    {
        public ValidateOptionsResult Validate(string? name, EntityFrameworkCoreOutboxOptions options)
        {
            try { _ = new OutboxConfiguration(options); return ValidateOptionsResult.Success; }
            catch (ArgumentException error) { return ValidateOptionsResult.Fail(error.Message); }
        }
    }

    // The configuration binding generator skips init-only setters; match the health integration's explicit binding.
    private sealed class OutboxOptionsFactory(IConfiguration configuration,
        IEnumerable<IConfigureOptions<EntityFrameworkCoreOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<EntityFrameworkCoreOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<EntityFrameworkCoreOutboxOptions>> validations)
        : OptionsFactory<EntityFrameworkCoreOutboxOptions>(setups, postConfigures, validations)
    {
        protected override EntityFrameworkCoreOutboxOptions CreateInstance(string name)
        {
            var defaults = new EntityFrameworkCoreOutboxOptions();
            return new()
            {
                Schema = configuration[nameof(defaults.Schema)],
                TablePrefix = configuration[nameof(defaults.TablePrefix)] ?? defaults.TablePrefix,
                Lanes = Read(nameof(defaults.Lanes), defaults.Lanes),
                DispatchStoreName = configuration[nameof(defaults.DispatchStoreName)],
                LeaseDuration = Read(nameof(defaults.LeaseDuration), defaults.LeaseDuration),
                MaxDispatchAttempts = Read(nameof(defaults.MaxDispatchAttempts), defaults.MaxDispatchAttempts)
            };
        }

        private T Read<T>(string key, T fallback) where T : IParsable<T>
        {
            var value = configuration[key];
            if (value is null) return fallback;
            if (T.TryParse(value, CultureInfo.InvariantCulture, out var parsed)) return parsed;
            throw new OptionsValidationException(Options.DefaultName, typeof(EntityFrameworkCoreOutboxOptions),
                [$"{key}: '{value}' is invalid."]);
        }
    }
}
