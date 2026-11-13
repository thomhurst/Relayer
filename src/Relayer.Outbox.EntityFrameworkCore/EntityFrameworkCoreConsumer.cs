using Relayer.Transports;
using System.Data;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class EntityFrameworkCoreConsumer<TContext>(string consumer, bool inbox, bool outbox,
    OutboxConfiguration configuration, RelayerInstrumentation instrumentation, IsolationLevel? isolationLevel = null,
    bool scopedValidation = false, bool explicitOptions = false, bool requireOutbox = false) : IConsumeTransactionMiddleware where TContext : DbContext
{
    private readonly IsolationLevel _isolationLevel = isolationLevel ?? IsolationLevel.Unspecified;

    internal bool ScopedValidation => scopedValidation;

    internal EntityFrameworkCoreConsumerSettings Resolve(DbContext db) => scopedValidation
        ? OutboxConfiguration.ValidateConsumer(db, inbox, configuration, explicitOptions, requireOutbox)
        : new(inbox, outbox, configuration);

    public ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
    {
        var db = context.Services.GetRequiredService<TContext>();
        return InvokeAsync(context, next, db, Resolve(db));
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next, TContext db, EntityFrameworkCoreConsumerSettings settings)
    {
        var cancellation = context.CancellationToken;
        var messageId = settings.Inbox ? ParseMessageId(context.MessageId) : default;
        if (db.Database.CurrentTransaction is not null || System.Transactions.Transaction.Current is not null)
            throw new InvalidOperationException("The EF Core consumer must own its transaction. Do not nest database consumer middleware or use an ambient transaction.");
        var transaction = await db.Database.BeginTransactionAsync(_isolationLevel, cancellation).ConfigureAwait(false);
        try
        {
            if (settings.Inbox)
            {
                // An outbox context can bind reloaded options in a new scope. Its interceptor has registered
                // the exact configuration with this transaction; never write the inbox to a different schema.
                var current = settings.Outbox ? context.Services.GetRequiredService<EntityFrameworkCoreOutbox>().Get(db, settings.Configuration).Configuration : settings.Configuration;
                // The native provider insert waits on the unique key for overlapping transactions.
                if (await current.InsertInboxAsync(db.Database.GetDbConnection(), transaction.GetDbTransaction(),
                        consumer, messageId, cancellation).ConfigureAwait(false) == 0)
                {
                    RequestCompletion(context);
                    await transaction.CommitAsync(cancellation).ConfigureAwait(false);
                    instrumentation.RecordInboxDuplicate(consumer);
                    return;
                }
            }
            await next(context).ConfigureAwait(false);
            if (!ReferenceEquals(db.Database.CurrentTransaction, transaction))
                throw new InvalidOperationException("The handler ended or replaced the consumer transaction. Save changes in the handler and let Relayer commit.");
            var disposition = context.Features.Get<ISettlementFeature>()?.RequestedDisposition;
            if (disposition is { Kind: not DispositionKind.Complete })
                await transaction.RollbackAsync(cancellation).ConfigureAwait(false);
            else
            {
                RequestCompletion(context);
                await transaction.CommitAsync(cancellation).ConfigureAwait(false);
            }
        }
        finally
        {
            // Disposal rolls back on every exception and cancellation path before failure handling runs.
            await transaction.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void RequestCompletion(MessageContext context)
    {
        // Fix the outcome before commit. Scope disposers cannot reject work that is already durable.
        if (context.Features.Get<ISettlementFeature>() is { RequestedDisposition: null } settlement)
            settlement.Complete();
    }

    internal static Guid ParseMessageId(string messageId)
    {
        if (!Guid.TryParse(messageId, out var id) || id == Guid.Empty || id.Version != 7)
            throw new MessageEnvelopeException("Inbox processing requires the original non-empty UUIDv7 message ID.");
        return id;
    }
}
