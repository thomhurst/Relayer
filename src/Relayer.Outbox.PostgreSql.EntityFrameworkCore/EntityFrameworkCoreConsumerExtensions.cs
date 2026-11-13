using Relayer.Transports;
using Relayer.Pipeline;
using System.Data;
using Microsoft.EntityFrameworkCore;
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

    /// <summary>Attaches the existing transaction and optional inbox boundary to a fluent consumer declaration. Register first.</summary>
    public static IConsumerBuilder EntityFrameworkCore<TContext>(this IConsumerBuilder builder,
        string consumer, bool inbox = false, EntityFrameworkCoreOutboxOptions? options = null,
        IsolationLevel? isolationLevel = null) where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        return builder.ConfigurePipeline(pipeline => pipeline.EntityFrameworkCore<TContext>(consumer, inbox, options, isolationLevel));
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
            var fallback = new PostgreSqlOutboxConfiguration(options ?? new());
            var scoped = new EntityFrameworkCoreConsumer<TContext>(consumer, inbox, false, fallback,
                instrumentation, isolationLevel, scopedValidation: true, explicitOptions: options is not null,
                requireOutbox: requireOutbox);
            return new RetryingEntityFrameworkCoreConsumer<TContext>(scoped, consumer, conflictRetries: isolationLevel is not null);
        }
        var context = services.GetRequiredService<TContext>();
        var settings = OutboxConfiguration.ValidateConsumer(context, inbox, new PostgreSqlOutboxConfiguration(options ?? new()), options is not null, requireOutbox);
        var boundary = new EntityFrameworkCoreConsumer<TContext>(consumer, settings.Inbox, settings.Outbox,
            settings.Configuration, instrumentation, isolationLevel);
        var nativeRetries = context.Database.CreateExecutionStrategy().RetriesOnFailure;
        return nativeRetries || isolationLevel is not null
            ? new RetryingEntityFrameworkCoreConsumer<TContext>(boundary, consumer, conflictRetries: !nativeRetries)
            : boundary;
    }
}
