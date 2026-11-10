using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

/// <summary>Configures an atomic MySQL consumer unit of work.</summary>
public static class MySqlConsumerExtensions
{
    /// <summary>Commits business writes, inbox identity and outgoing messages before settlement.</summary>
    /// <remarks>
    /// Register AddRelayerMySqlOutbox and add this boundary before other middleware. The data source
    /// remains application-owned. Use the scoped MySqlOutbox.CurrentTransaction.Connection for business
    /// commands; handlers must not complete the transaction. Inbox deduplication is opt-in.
    /// Consumer identities are stable UTF-8 strings of at most 768 bytes. Batch consumers are unsupported.
    /// </remarks>
    public static ConsumePipelineBuilder MySql(this ConsumePipelineBuilder builder, MySqlDataSource dataSource,
        string consumer, bool inbox = false, IsolationLevel isolationLevel = IsolationLevel.Unspecified)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        // Strict encoding prevents two invalid UTF-16 identities from becoming the same replacement bytes.
        var identity = new UTF8Encoding(false, true).GetBytes(consumer);
        if (identity.Length > 768) throw new ArgumentException("MySQL consumer identities must fit in 768 UTF-8 bytes.", nameof(consumer));
        return builder.Use(services =>
        {
            _ = services.GetRequiredService<MySqlOutbox>();
            return new MySqlConsumer(dataSource, consumer, identity, inbox, isolationLevel,
                services.GetRequiredService<RelayerInstrumentation>());
        });
    }
}

internal sealed class MySqlConsumer(MySqlDataSource dataSource, string consumer, byte[] identity, bool inbox,
    IsolationLevel isolationLevel, RelayerInstrumentation instrumentation) : IConsumeTransactionMiddleware
{
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
    {
        var id = inbox ? ParseMessageId(context.MessageId) : default;
        var cancellation = context.CancellationToken;
        var outbox = context.Services.GetRequiredService<MySqlOutbox>();
        await using var transaction = (MySqlOutboxTransaction)await outbox.BeginTransactionAsync(dataSource, isolationLevel, cancellation).ConfigureAwait(false);
        transaction.ConsumerOwned = true;
        if (inbox && !await outbox.TryInsertInboxAsync(transaction, identity, id, cancellation).ConfigureAwait(false))
        {
            RequestCompletion(context);
            await transaction.CommitConsumerAsync(cancellation).ConfigureAwait(false);
            instrumentation.RecordInboxDuplicate(consumer);
            return;
        }
        await next(context).ConfigureAwait(false);
        if (!ReferenceEquals(outbox.CurrentTransaction, transaction))
            throw new InvalidOperationException("The handler disposed the MySQL consumer transaction. Let Relayer own its lifecycle.");
        cancellation.ThrowIfCancellationRequested();
        if (context.Features.Get<ISettlementFeature>()?.RequestedDisposition is { Kind: not DispositionKind.Complete })
            return; // Native disposal rolls back before ordinary failure handling and settlement.
        RequestCompletion(context);
        await transaction.CommitConsumerAsync(cancellation).ConfigureAwait(false);
    }

    private static Guid ParseMessageId(string messageId)
    {
        if (!Guid.TryParse(messageId, out var id) || id == Guid.Empty || id.Version != 7)
            throw new MessageEnvelopeException("Inbox processing requires the original non-empty UUIDv7 message ID.");
        return id;
    }

    private static void RequestCompletion(MessageContext context)
    {
        if (context.Features.Get<ISettlementFeature>() is { RequestedDisposition: null } settlement)
            settlement.Complete();
    }
}
