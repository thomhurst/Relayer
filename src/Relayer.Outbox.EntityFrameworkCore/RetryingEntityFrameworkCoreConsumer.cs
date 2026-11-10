using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
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
            // Cleanup belongs outside both native retries and handler-failure policy.
            if (incoming.ScopeCleanupError is { } cleanup) ExceptionDispatchInfo.Throw(cleanup);
        }
        catch
        {
            incoming.SendBuffer.Clear();
            throw;
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
        // A new native attempt means verification did not establish a committed result.
        context.SendBuffer.Clear();
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
                var scopedSender = context.Services.GetService<ISender>();
                if (scopedSender is not null && scopedSender is not Sender)
                    throw new InvalidOperationException("EF Core consumer retries require Relayer's scoped ISender.");
                sender = (Sender?)scopedSender;
                if (sender is not null) sender.AttemptBuffer = buffer;
                try { await boundary.InvokeAsync(context, operation.Next).ConfigureAwait(false); }
                finally
                {
                    // The transaction interceptor registers this scope's configuration first, including
                    // when commit throws. Verification must query that attempted inbox, not startup options.
                    operation.Configuration = outbox
                        ? context.Services.GetRequiredService<EntityFrameworkCoreOutbox>().Get(db, configuration).Configuration
                        : configuration;
                }
            }
            finally
            {
                try { await scope.DisposeAsync().ConfigureAwait(false); }
                catch (Exception error) { context.ScopeCleanupError = error; }
                finally { sender?.EndAttempt(); }
            }
            return true;
        }
        catch when (context.ScopeCleanupError is not null)
        {
            // Stop the strategy even if cleanup replaced a failed transaction. InvokeAsync propagates it.
            return true;
        }
        catch
        {
            context.Settlement.Clear();
            // Preserve sends until native unknown-commit verification has finished.
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
        var committed = false;
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
                committed = (bool)(await command.ExecuteScalarAsync(cancellation).ConfigureAwait(false))!;
                // Inbox rows commit only on the completion path.
                if (committed && context.Settlement.Request is null) context.Settlement.Complete();
            }
            finally
            {
                try { await scope.DisposeAsync().ConfigureAwait(false); }
                catch (Exception error) { context.ScopeCleanupError = error; }
            }
        }
        catch when (context.ScopeCleanupError is not null) { }
        finally { context.SetServices(services); }
        return new(committed || context.ScopeCleanupError is not null, true);
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
