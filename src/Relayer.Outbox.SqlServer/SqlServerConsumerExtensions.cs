using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Relayer.Outbox.SqlServer;

/// <summary>Configures atomic SQL Server handling with the scoped ADO.NET outbox.</summary>
public static class SqlServerConsumerExtensions
{
    /// <summary>Owns one transaction around handling, optionally deduplicating by consumer and original UUIDv7.</summary>
    /// <remarks>
    /// Register first. Register AddRelayerSqlServerOutbox and use SqlServerOutbox.CurrentTransaction.Connection
    /// for business commands. Handlers must not complete or replace the transaction. No EF Core integration,
    /// automatic retry strategy, ambient transactions or batch consumers are supported.
    /// The native connection string passes through unchanged. Keep consumer identities stable across replicas.
    /// </remarks>
    [RequiresUnreferencedCode("SqlClient 7.1.1 runtime trimming is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    [RequiresDynamicCode("SqlClient 7.1.1 runtime Native AOT is unsupported; see https://github.com/dotnet/SqlClient/issues/1947.")]
    public static ConsumePipelineBuilder SqlServer(this ConsumePipelineBuilder builder, string consumer,
        string connectionString, bool inbox = false)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        if (consumer.Length > 256 || consumer.EndsWith(' '))
            throw new ArgumentException("Consumer identities must fit 256 UTF-16 code units and must not end in a space.", nameof(consumer));
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = new SqlConnectionStringBuilder(connectionString);
        return builder.Use(services =>
        {
            var outbox = services.GetRequiredService<SqlServerOutbox>();
            var options = services.GetRequiredService<IOptionsSnapshot<SqlServerOutboxOptions>>().Value;
            var table = SqlServerOutboxSchema.InboxTable(options);
            var insert = inbox ? $"INSERT INTO {table} (consumer, message_id) SELECT @consumer, @id WHERE NOT EXISTS (SELECT 1 FROM {table} WITH (UPDLOCK, HOLDLOCK) WHERE consumer = @consumer AND message_id = @id); SELECT @@ROWCOUNT" : null;
            return new Boundary(consumer, connectionString, outbox.Table, insert, services.GetRequiredService<RelayerInstrumentation>());
        });
    }

    private sealed class Boundary(string consumer, string connectionString, string outboxTable, string? insert,
        RelayerInstrumentation instrumentation) : IConsumeTransactionMiddleware
    {
        [RuntimeAsyncMethodGeneration(false)]
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
        {
            var id = default(Guid);
            if (insert is not null && (!Guid.TryParse(context.MessageId, out id) || id == Guid.Empty || id.Version != 7))
                throw new MessageEnvelopeException("Inbox processing requires the original non-empty UUIDv7 message ID.");
            var outbox = context.Services.GetRequiredService<SqlServerOutbox>();
            if (outbox.Table != outboxTable)
                throw new InvalidOperationException("SQL Server consumer schema or prefix changed after composition. Restart the consumer pipeline with matching deployed outbox options.");
            var cancellation = context.CancellationToken;
            await using var transaction = (SqlServerOutboxTransaction)await outbox.BeginTransactionAsync(
                connectionString, IsolationLevel.ReadCommitted, cancellation).ConfigureAwait(false);
            transaction.ConsumerOwned = true;
            if (insert is not null)
            {
                // Native serializable key-range locking waits for an overlapping owner before checking its committed key.
                // UPDLOCK prevents two absent-key readers from taking shared ranges and then deadlocking on insert.
                var native = transaction.Unwrap(transaction.Connection);
                await using var command = new SqlCommand(insert, native.Connection, native);
                var consumerParameter = command.CreateParameter();
                consumerParameter.ParameterName = "@consumer";
                consumerParameter.DbType = DbType.String;
                consumerParameter.Size = 256;
                consumerParameter.Value = consumer;
                command.Parameters.Add(consumerParameter);
                var idParameter = command.CreateParameter();
                idParameter.ParameterName = "@id";
                idParameter.DbType = DbType.Guid;
                idParameter.Value = id;
                command.Parameters.Add(idParameter);
                // Explicit SQL result remains available when the session suppresses affected-row tokens with NOCOUNT.
                if ((int)(await command.ExecuteScalarAsync(cancellation).ConfigureAwait(false))! == 0)
                {
                    RequestCompletion(context);
                    transaction.ConsumerOwned = false;
                    await transaction.CommitAsync(cancellation).ConfigureAwait(false);
                    instrumentation.RecordInboxDuplicate(consumer);
                    return;
                }
            }
            await next(context).ConfigureAwait(false);
            if (!ReferenceEquals(outbox.CurrentTransaction, transaction))
                throw new InvalidOperationException("The handler ended the SQL Server consumer transaction. Let Relayer complete it.");
            transaction.ConsumerOwned = false;
            if (context.Features.Get<ISettlementFeature>()?.RequestedDisposition is { Kind: not DispositionKind.Complete })
                await transaction.RollbackAsync(cancellation).ConfigureAwait(false);
            else
            {
                RequestCompletion(context);
                await transaction.CommitAsync(cancellation).ConfigureAwait(false);
            }
        }

        private static void RequestCompletion(MessageContext context)
        {
            if (context.Features.Get<ISettlementFeature>() is { RequestedDisposition: null } settlement)
                settlement.Complete();
        }
    }
}
