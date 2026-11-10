using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Configures a consumer's explicit database unit of work.</summary>
public static class EntityFrameworkCoreConsumerExtensions
{
    /// <summary>Wraps handling in one PostgreSQL transaction. Inbox is automatic with AddRelayerOutbox, otherwise opt in.</summary>
    /// <remarks>
    /// Register first, before other middleware. Handlers save changes but must not commit, roll back or replace this transaction.
    /// Request retry or dead-letter before returning from handling. Before commit the boundary requests completion
    /// if no outcome is pending; further settlement requests, including during scope disposal, throw.
    /// With AddRelayerTenancy, the application authorizes and selects its native scoped DbContext on delivery;
    /// provider and transaction settings are validated in that scope instead of resolving a startup context.
    /// Retrying EF execution strategies replay the complete inner pipeline in a fresh scope per attempt.
    /// An explicit isolation level enables up to three immediate PostgreSQL conflict retries when no native retry strategy is configured.
    /// Use a stable consumer identity across replicas and restarts. Batch consumers are not supported.
    /// </remarks>
    public static ConsumePipelineBuilder EntityFrameworkCore<TContext>(this ConsumePipelineBuilder builder,
        string consumer, bool inbox = false, EntityFrameworkCoreOutboxOptions? options = null,
        IsolationLevel? isolationLevel = null) where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        return builder.Use(services => Create<TContext>(services, consumer, inbox, options, isolationLevel: isolationLevel));
    }

    internal static IConsumeMiddleware Create<TContext>(IServiceProvider services, string consumer,
        bool inbox = false, EntityFrameworkCoreOutboxOptions? options = null, bool requireOutbox = false,
        IsolationLevel? isolationLevel = null) where TContext : DbContext
    {
        var instrumentation = services.GetRequiredService<RelayerInstrumentation>();
        // A tenant exists only in a delivery scope. Native AddDbContext factories may reject an unset
        // tenant, so do not invoke them during composition. Fixed-store consumers retain early validation.
        if (services.GetService<TenantContext>() is not null)
        {
            var fallback = new OutboxConfiguration(options ?? new());
            var scoped = new EntityFrameworkCoreConsumer<TContext>(consumer, inbox, false, fallback,
                instrumentation, isolationLevel, scopedValidation: true, explicitOptions: options is not null,
                requireOutbox: requireOutbox);
            return new RetryingEntityFrameworkCoreConsumer<TContext>(scoped, consumer, conflictRetries: isolationLevel is not null);
        }
        var context = services.GetRequiredService<TContext>();
        var settings = Validate(context, inbox, new OutboxConfiguration(options ?? new()), options is not null, requireOutbox);
        var boundary = new EntityFrameworkCoreConsumer<TContext>(consumer, settings.Inbox, settings.Outbox,
            settings.Configuration, instrumentation, isolationLevel);
        var nativeRetries = context.Database.CreateExecutionStrategy().RetriesOnFailure;
        return nativeRetries || isolationLevel is not null
            ? new RetryingEntityFrameworkCoreConsumer<TContext>(boundary, consumer, conflictRetries: !nativeRetries)
            : boundary;
    }

    internal static EntityFrameworkCoreConsumerSettings Validate(DbContext context, bool inbox,
        OutboxConfiguration fallback, bool explicitOptions, bool requireOutbox)
    {
        if (context.Database.ProviderName != "Npgsql.EntityFrameworkCore.PostgreSQL")
            throw new InvalidOperationException("EF Core consumers currently require the Npgsql PostgreSQL provider.");
        var nativeOptions = context.GetService<IDbContextOptions>();
        OutboxConfiguration.ValidatePooling(nativeOptions);
        var outbox = nativeOptions.FindExtension<OutboxConfiguration>();
        if (outbox is not null && explicitOptions)
            throw new InvalidOperationException("Configure schema and prefix on AddRelayerOutbox; the consumer inbox uses that same configuration.");
        if (requireOutbox && outbox is null)
            throw new InvalidOperationException("Saga consumers require AddRelayerOutbox on the selected scoped DbContext. State, inbox and outgoing messages must share one transaction.");
        return new(inbox || outbox is not null, outbox is not null, outbox ?? fallback);
    }
}

internal readonly record struct EntityFrameworkCoreConsumerSettings(bool Inbox, bool Outbox, OutboxConfiguration Configuration);
