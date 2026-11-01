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
    /// Use a stable consumer identity across replicas and restarts. Batch consumers are not supported.
    /// </remarks>
    public static ConsumePipelineBuilder EntityFrameworkCore<TContext>(this ConsumePipelineBuilder builder,
        string consumer, bool inbox = false, EntityFrameworkCoreOutboxOptions? options = null) where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        return builder.Use(services =>
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
            var configuration = outbox ?? new OutboxConfiguration(options ?? new());
            return new EntityFrameworkCoreConsumer<TContext>(consumer, inbox || outbox is not null, outbox is not null,
                configuration, services.GetRequiredService<RelayerInstrumentation>());
        });
    }
}
