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
    /// Deploy the outbox schema before starting the host. For independent worker deployment, use the individual registrations instead.
    /// </remarks>
    public static IRelayerBuilder UsePostgreSqlOutbox<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors |
            DynamicallyAccessedMemberTypes.NonPublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties)] TContext>(this IRelayerBuilder builder,
        NpgsqlDataSource dataSource, EntityFrameworkCoreOutboxOptions? options = null)
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
        var store = new PostgreSqlOutboxOptions
        {
            Schema = writer.Schema, TablePrefix = writer.TablePrefix, Lanes = writer.Lanes,
            LeaseDuration = writer.LeaseDuration, MaxDispatchAttempts = writer.MaxDispatchAttempts
        };
        var ordered = new PostgreSqlOrderedDispatchOptions
        {
            Schema = writer.Schema, TablePrefix = writer.TablePrefix, Lanes = writer.Lanes,
            LeaseDuration = writer.LeaseDuration, MaxDispatchAttempts = writer.MaxDispatchAttempts
        };
        // Validate before mutating the service collection.
        _ = new OutboxConfiguration(writer);
        _ = ordered.Validate();
        var services = builder.Services;
        services.AddRelayerEntityFrameworkCoreOutbox();
        // A selected context always uses its own settings. The first composed registration supplies
        // defaults for capability validation before a context is selected, unless the app configured them.
        services.TryAddSingleton<IOptionsFactory<EntityFrameworkCoreOutboxOptions>>(provider => new WriterOptionsFactory(writer,
            provider.GetServices<IConfigureOptions<EntityFrameworkCoreOutboxOptions>>(),
            provider.GetServices<IPostConfigureOptions<EntityFrameworkCoreOutboxOptions>>(),
            provider.GetServices<IValidateOptions<EntityFrameworkCoreOutboxOptions>>()));
        services.AddRelayerPostgreSqlDirectDispatch(name, dataSource, store);
        services.AddRelayerPostgreSqlOrderedDispatch(name, dataSource, ordered);
        services.AddRelayerPostgreSqlMaintenance(name, dataSource, new PostgreSqlMaintenanceOptions
        {
            Schema = writer.Schema, TablePrefix = writer.TablePrefix,
            MaxDispatchAttempts = writer.MaxDispatchAttempts
        });
        services.AddDbContext<TContext>((provider, native) => native.AddRelayerOutbox(provider, writer));
        return builder;
    }

    private sealed class WriterOptionsFactory(EntityFrameworkCoreOutboxOptions options,
        IEnumerable<IConfigureOptions<EntityFrameworkCoreOutboxOptions>> setups,
        IEnumerable<IPostConfigureOptions<EntityFrameworkCoreOutboxOptions>> postConfigures,
        IEnumerable<IValidateOptions<EntityFrameworkCoreOutboxOptions>> validations)
        : OptionsFactory<EntityFrameworkCoreOutboxOptions>(setups, postConfigures, validations)
    {
        protected override EntityFrameworkCoreOutboxOptions CreateInstance(string name) => options;
    }
}
