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
    /// Use a stable consumer identity across replicas and restarts. Batch consumers are not supported.
    /// </remarks>
    public static ConsumePipelineBuilder EntityFrameworkCore<TContext>(this ConsumePipelineBuilder builder,
        string consumer, bool inbox = false, EntityFrameworkCoreOutboxOptions? options = null) where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        return builder.Use(services => Create<TContext>(services, consumer, inbox, options));
    }

    internal static IConsumeMiddleware Create<TContext>(IServiceProvider services, string consumer,
        bool inbox = false, EntityFrameworkCoreOutboxOptions? options = null, bool requireOutbox = false) where TContext : DbContext
    {
        var context = services.GetRequiredService<TContext>();
        if (context.Database.ProviderName != "Npgsql.EntityFrameworkCore.PostgreSQL")
            throw new InvalidOperationException("EF Core consumers currently require the Npgsql PostgreSQL provider.");
        if (context.Database.CreateExecutionStrategy().RetriesOnFailure)
            throw new InvalidOperationException("EF Core consumers require retries of the complete handler in a fresh DI scope. Disable EnableRetryOnFailure and configure Relayer consumer retries.");
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
        return new EntityFrameworkCoreConsumer<TContext>(consumer, inbox || outbox is not null, outbox is not null,
            configuration, services.GetRequiredService<RelayerInstrumentation>());
    }
}
