using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Composes EF Core enlistment and PostgreSQL dispatch with durable recovery.</summary>
public static class PostgreSqlOutboxBuilderExtensions
{
    /// <summary>Enlists one scoped DbContext and registers direct dispatch, ordered dispatch and recovery.</summary>
    /// <remarks>
    /// Configure the native provider with AddDbContext separately, using the same application-owned data source.
    /// Call again for another context with the same data source and options to enlist it in the same store and workers.
    /// Deploy the outbox schema before starting the host. For independent worker deployment, use the individual registrations instead.
    /// The worker options tune direct dispatch, ordered dispatch and maintenance. Schema, table prefix, lanes, the ordered
    /// lease duration and the attempt limit come from <paramref name="options"/>; leave them unset on the worker options.
    /// </remarks>
    public static IRelayerBuilder UsePostgreSqlOutbox<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.NonPublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties)] TContext>(this IRelayerBuilder builder,
        NpgsqlDataSource dataSource, EntityFrameworkCoreOutboxOptions? options = null,
        PostgreSqlDirectDispatchOptions? directDispatch = null, PostgreSqlOrderedDispatchOptions? orderedDispatch = null,
        PostgreSqlMaintenanceOptions? maintenance = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(dataSource);
        options ??= new();
        if (options.TenantBinding is not null)
            throw new ArgumentException("Configure tenant-bound outboxes in the authorized scoped native store factory instead.", nameof(options));
        var name = options.DispatchStoreName ?? "default";
        var writer = new EntityFrameworkCoreOutboxOptions
        {
            Schema = options.Schema, TablePrefix = options.TablePrefix, Lanes = options.Lanes,
            DispatchStoreName = name, LeaseDuration = options.LeaseDuration, MaxDispatchAttempts = options.MaxDispatchAttempts
        };
        var orderedSettings = orderedDispatch ?? new();
        var maintenanceSettings = maintenance ?? new();
        PostgreSqlOrderedDispatchOptions orderedDefaults = new();
        PostgreSqlMaintenanceOptions maintenanceDefaults = new();
        RejectStoreSetting(orderedSettings.Schema, orderedDefaults.Schema, writer.Schema, nameof(orderedDispatch), nameof(orderedSettings.Schema));
        RejectStoreSetting(orderedSettings.TablePrefix, orderedDefaults.TablePrefix, writer.TablePrefix, nameof(orderedDispatch), nameof(orderedSettings.TablePrefix));
        RejectStoreSetting(orderedSettings.Lanes, orderedDefaults.Lanes, writer.Lanes, nameof(orderedDispatch), nameof(orderedSettings.Lanes));
        RejectStoreSetting(orderedSettings.LeaseDuration, orderedDefaults.LeaseDuration, writer.LeaseDuration, nameof(orderedDispatch), nameof(orderedSettings.LeaseDuration));
        RejectStoreSetting(orderedSettings.MaxDispatchAttempts, orderedDefaults.MaxDispatchAttempts, writer.MaxDispatchAttempts, nameof(orderedDispatch), nameof(orderedSettings.MaxDispatchAttempts));
        RejectStoreSetting(maintenanceSettings.Schema, maintenanceDefaults.Schema, writer.Schema, nameof(maintenance), nameof(maintenanceSettings.Schema));
        RejectStoreSetting(maintenanceSettings.TablePrefix, maintenanceDefaults.TablePrefix, writer.TablePrefix, nameof(maintenance), nameof(maintenanceSettings.TablePrefix));
        RejectStoreSetting(maintenanceSettings.MaxDispatchAttempts, maintenanceDefaults.MaxDispatchAttempts, writer.MaxDispatchAttempts, nameof(maintenance), nameof(maintenanceSettings.MaxDispatchAttempts));
        var retainSent = maintenanceSettings.RetainSentMessages;
        if (orderedSettings.RetainSentMessages != retainSent)
            throw new ArgumentException("Set RetainSentMessages to the same value on the ordered dispatch and maintenance options.", nameof(maintenance));
        var store = new PostgreSqlOutboxOptions
        {
            Schema = writer.Schema, TablePrefix = writer.TablePrefix, Lanes = writer.Lanes,
            LeaseDuration = writer.LeaseDuration, MaxDispatchAttempts = writer.MaxDispatchAttempts, RetainSentMessages = retainSent
        };
        var ordered = new PostgreSqlOrderedDispatchOptions
        {
            Schema = writer.Schema, TablePrefix = writer.TablePrefix, Lanes = writer.Lanes,
            LeaseDuration = writer.LeaseDuration, MaxDispatchAttempts = writer.MaxDispatchAttempts,
            PollInterval = orderedSettings.PollInterval, BatchSize = orderedSettings.BatchSize, RetainSentMessages = retainSent
        };
        var recovery = new PostgreSqlMaintenanceOptions
        {
            Schema = writer.Schema, TablePrefix = writer.TablePrefix, MaxDispatchAttempts = writer.MaxDispatchAttempts,
            RetainSentMessages = retainSent, BatchSize = maintenanceSettings.BatchSize,
            CleanupBatchLimit = maintenanceSettings.CleanupBatchLimit, RecoveryDelay = maintenanceSettings.RecoveryDelay,
            SweepInterval = maintenanceSettings.SweepInterval, LeaseDuration = maintenanceSettings.LeaseDuration,
            ClaimDuration = maintenanceSettings.ClaimDuration, CleanupInterval = maintenanceSettings.CleanupInterval,
            DeadRetention = maintenanceSettings.DeadRetention, InboxRetention = maintenanceSettings.InboxRetention,
            SentRetention = maintenanceSettings.SentRetention
        };
        // Validate before mutating the service collection.
        _ = new PostgreSqlOutboxConfiguration(writer);
        _ = ordered.Validate();
        _ = recovery.Validate();
        var services = builder.Services;
        var composed = new ComposedStore(name, dataSource, writer.Schema, writer.TablePrefix, writer.Lanes,
            writer.LeaseDuration, writer.MaxDispatchAttempts, directDispatch, orderedDispatch, maintenance);
        foreach (var descriptor in services)
        {
            if (descriptor.ImplementationInstance is not ComposedStore existing || existing.Name != name) continue;
            if (existing != composed)
                throw new InvalidOperationException($"PostgreSQL outbox store '{name}' is already composed with a different NpgsqlDataSource or options. " +
                    "Pass the same data source and options to share the store, or set a distinct DispatchStoreName and separate store tables.");
            services.AddDbContext<TContext>((provider, native) => native.AddRelayerOutbox(provider, writer));
            return builder;
        }
        services.AddSingleton(composed);
        services.AddRelayerEntityFrameworkCoreOutbox();
        // A selected context always uses its own settings. The first composed registration supplies
        // defaults for capability validation before a context is selected, unless the app configured them.
        services.TryAddSingleton<IOptionsFactory<EntityFrameworkCoreOutboxOptions>>(provider => new WriterOptionsFactory(writer,
            provider.GetServices<IConfigureOptions<EntityFrameworkCoreOutboxOptions>>(),
            provider.GetServices<IPostConfigureOptions<EntityFrameworkCoreOutboxOptions>>(),
            provider.GetServices<IValidateOptions<EntityFrameworkCoreOutboxOptions>>()));
        services.AddRelayerPostgreSqlDirectDispatch(name, dataSource, store, directDispatch);
        services.AddRelayerPostgreSqlOrderedDispatch(name, dataSource, ordered);
        services.AddRelayerPostgreSqlMaintenance(name, dataSource, recovery);
        services.AddDbContext<TContext>((provider, native) => native.AddRelayerOutbox(provider, writer));
        return builder;
    }

    // Store settings belong to the writer options; a worker option may only repeat them.
    private static void RejectStoreSetting<T>(T value, T unset, T store, string parameter, string property)
    {
        if (!EqualityComparer<T>.Default.Equals(value, unset) && !EqualityComparer<T>.Default.Equals(value, store))
            throw new ArgumentException($"Set {property} on EntityFrameworkCoreOutboxOptions; it applies to every worker of the store.", parameter);
    }

    // Store identity for composed registrations; the data source and worker options compare by reference.
    private sealed record ComposedStore(string Name, NpgsqlDataSource DataSource, string? Schema, string TablePrefix,
        int Lanes, TimeSpan LeaseDuration, int MaxDispatchAttempts, PostgreSqlDirectDispatchOptions? DirectDispatch,
        PostgreSqlOrderedDispatchOptions? OrderedDispatch, PostgreSqlMaintenanceOptions? Maintenance);

    private sealed class WriterOptionsFactory(EntityFrameworkCoreOutboxOptions options,
        IEnumerable<IConfigureOptions<EntityFrameworkCoreOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<EntityFrameworkCoreOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<EntityFrameworkCoreOutboxOptions>> validations)
        : OptionsFactory<EntityFrameworkCoreOutboxOptions>(setups, postConfigures, validations)
    {
        protected override EntityFrameworkCoreOutboxOptions CreateInstance(string name) => options;
    }
}
