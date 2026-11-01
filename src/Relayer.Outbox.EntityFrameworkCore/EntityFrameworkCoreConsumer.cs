using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class EntityFrameworkCoreConsumer<TContext>(string consumer, bool inbox, bool outbox,
    OutboxConfiguration configuration, RelayerInstrumentation instrumentation) : IConsumeTransactionMiddleware where TContext : DbContext
{
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
    {
        var cancellation = context.CancellationToken;
        var messageId = inbox ? ParseMessageId(context.MessageId) : default;
        var db = context.Services.GetRequiredService<TContext>();
        if (db.Database.CurrentTransaction is not null || System.Transactions.Transaction.Current is not null)
            throw new InvalidOperationException("The EF Core consumer must own its transaction. Do not nest database consumer middleware or use an ambient transaction.");
        var transaction = await db.Database.BeginTransactionAsync(cancellation).ConfigureAwait(false);
        try
        {
            if (inbox)
            {
                // An outbox context can bind reloaded options in a new scope. Its interceptor has registered
                // the exact configuration with this transaction; never write the inbox to a different schema.
                var current = outbox ? context.Services.GetRequiredService<EntityFrameworkCoreOutbox>().Get(db, configuration).Configuration : configuration;
                await using var command = new NpgsqlCommand(current.InboxInsertSql,
                    (NpgsqlConnection)db.Database.GetDbConnection(), (NpgsqlTransaction)transaction.GetDbTransaction());
                command.Parameters.Add(new NpgsqlParameter<string>("consumer", consumer));
                command.Parameters.Add(new NpgsqlParameter<Guid>("id", messageId));
                // The unique key waits for an overlapping transaction. A rollback lets this insert succeed.
                if (await command.ExecuteNonQueryAsync(cancellation).ConfigureAwait(false) == 0)
                {
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
                await transaction.CommitAsync(cancellation).ConfigureAwait(false);
        }
        finally
        {
            // Disposal rolls back on every exception and cancellation path before failure handling runs.
            await transaction.DisposeAsync().ConfigureAwait(false);
        }
    }

    internal static Guid ParseMessageId(string messageId)
    {
        if (!Guid.TryParse(messageId, out var id) || id == Guid.Empty || id.Version != 7)
            throw new MessageEnvelopeException("Inbox processing requires the original non-empty UUIDv7 message ID.");
        return id;
    }
}
