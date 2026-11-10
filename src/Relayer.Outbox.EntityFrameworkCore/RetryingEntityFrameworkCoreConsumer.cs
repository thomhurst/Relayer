using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Relayer.Outbox.EntityFrameworkCore;

// Selected once at composition; the non-retrying boundary pays none of these costs.
internal sealed class RetryingEntityFrameworkCoreConsumer<TContext>(EntityFrameworkCoreConsumer<TContext> boundary,
    string consumer, bool inbox, bool outbox, OutboxConfiguration configuration) : IConsumeTransactionMiddleware where TContext : DbContext
{
    private readonly Operation?[] _operations = new Operation[16];

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
    {
        var incoming = (DefaultMessageContext)context;
        var services = incoming.Services;
        var strategy = services.GetRequiredService<TContext>().Database.CreateExecutionStrategy();
        var messageId = inbox ? EntityFrameworkCoreConsumer<TContext>.ParseMessageId(context.MessageId) : default;
        Operation? operation = null;
        for (var i = 0; i < _operations.Length; i++)
            if (Interlocked.Exchange(ref _operations[i], null) is { } cached) { operation = cached; break; }
        operation ??= new Operation();
        operation.Context = incoming;
        operation.Next = next;
        operation.Configuration = configuration;
        operation.MessageId = messageId;
        try
        {
            await strategy.ExecuteAsync((Owner: this, Operation: operation),
                static (_, state, cancellation) => state.Owner.AttemptAsync(state.Operation, cancellation),
                inbox ? static (_, state, cancellation) => state.Owner.VerifyAsync(state.Operation, cancellation) : null,
                context.CancellationToken).ConfigureAwait(false);
        }
        finally
        {
            incoming.SetServices(services);
            operation.Clear();
            for (var i = 0; i < _operations.Length; i++)
                if (Interlocked.CompareExchange(ref _operations[i], operation, null) is null) break;
        }
    }

    private async Task<bool> AttemptAsync(Operation operation, CancellationToken cancellation)
    {
        var context = operation.Context;
        var scopes = context.Services.GetRequiredService<IServiceScopeFactory>();
        var services = context.Services;
        var scope = scopes.CreateAsyncScope();
        Sender? sender = null;
        try
        {
            try
            {
                var buffer = context.ActiveAttemptBuffer;
                context.SetServices(scope.ServiceProvider);
                var db = context.Services.GetRequiredService<TContext>();
                operation.Configuration = outbox
                    ? context.Services.GetRequiredService<EntityFrameworkCoreOutbox>().Get(db, configuration).Configuration
                    : configuration;
                var scopedSender = context.Services.GetService<ISender>();
                if (scopedSender is not null && scopedSender is not Sender)
                    throw new InvalidOperationException("EF Core consumer retries require Relayer's scoped ISender.");
                sender = (Sender?)scopedSender;
                if (sender is not null) sender.AttemptBuffer = buffer;
                await boundary.InvokeAsync(context, operation.Next).ConfigureAwait(false);
            }
            finally
            {
                try { await scope.DisposeAsync().ConfigureAwait(false); }
                finally { sender?.EndAttempt(); }
            }
            return true;
        }
        catch
        {
            context.Settlement.Clear();
            context.SendBuffer.Clear();
            throw;
        }
        finally { context.SetServices(services); }
    }

    private async Task<ExecutionResult<bool>> VerifyAsync(Operation operation, CancellationToken cancellation)
    {
        // Use a new connection, never the possibly broken transaction connection. EF applies its
        // native transient classification and retry policy to this verification delegate too.
        var context = operation.Context;
        var services = context.Services;
        var scope = services.GetRequiredService<IServiceScopeFactory>().CreateAsyncScope();
        try
        {
            try
            {
                // Restore tenant-scoped configuration before resolving the verification context too.
                context.SetServices(scope.ServiceProvider);
                var db = scope.ServiceProvider.GetRequiredService<TContext>();
                var connection = (NpgsqlConnection)db.Database.GetDbConnection();
                await connection.OpenAsync(cancellation).ConfigureAwait(false);
                await using var command = new NpgsqlCommand(operation.Configuration.InboxExistsSql, connection);
                command.Parameters.Add(new NpgsqlParameter<string>("consumer", consumer));
                command.Parameters.Add(new NpgsqlParameter<Guid>("id", operation.MessageId));
                var committed = (bool)(await command.ExecuteScalarAsync(cancellation).ConfigureAwait(false))!;
                if (committed && context.Settlement.Request is null) context.Settlement.Complete();
                return new(committed, true);
            }
            finally { await scope.DisposeAsync().ConfigureAwait(false); }
        }
        finally { context.SetServices(services); }
    }

    private sealed class Operation
    {
        internal DefaultMessageContext Context = null!;
        internal ConsumeDelegate Next = null!;
        internal OutboxConfiguration Configuration = null!;
        internal Guid MessageId;
        internal void Clear()
        {
            Context = null!;
            Next = null!;
            Configuration = null!;
            MessageId = default;
        }
    }
}
