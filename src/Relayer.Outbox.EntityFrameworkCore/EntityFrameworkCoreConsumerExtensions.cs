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
        var context = services.GetRequiredService<TContext>();
        if (context.Database.ProviderName != "Npgsql.EntityFrameworkCore.PostgreSQL")
            throw new InvalidOperationException("EF Core consumers currently require the Npgsql PostgreSQL provider.");
        var core = context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>();
        if (core?.MaxPoolSize is not null)
            throw new InvalidOperationException("EF Core consumers require scoped DbContexts. Use AddDbContext instead of AddDbContextPool.");
        OutboxConfiguration? outbox = null;
        if (core?.Interceptors is { } interceptors)
            foreach (var interceptor in interceptors)
                if (interceptor is OutboxSaveChangesInterceptor save) outbox = save.Configuration;
        if (outbox is not null && options is not null)
            throw new InvalidOperationException("Configure schema and prefix on AddRelayerOutbox; the consumer inbox uses that same configuration.");
        if (requireOutbox && outbox is null)
            throw new InvalidOperationException("Saga consumers require AddRelayerOutbox on the selected scoped DbContext. State, inbox and outgoing messages must share one transaction.");
        var configuration = outbox ?? new OutboxConfiguration(options ?? new());
        var boundary = new EntityFrameworkCoreConsumer<TContext>(consumer, inbox || outbox is not null, outbox is not null,
            configuration, services.GetRequiredService<RelayerInstrumentation>(), isolationLevel);
        var nativeRetries = context.Database.CreateExecutionStrategy().RetriesOnFailure;
        // Keep the provider-default path unchanged. Explicit isolation opts into bounded conflict replay.
        return nativeRetries || isolationLevel is not null
            ? new RetryingEntityFrameworkCoreConsumer<TContext>(boundary, consumer, inbox || outbox is not null,
                outbox is not null, configuration, conflictRetries: !nativeRetries)
            : boundary;
    }
}
