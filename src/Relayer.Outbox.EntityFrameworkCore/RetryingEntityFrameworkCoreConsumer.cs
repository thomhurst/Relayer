using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Pipeline;

namespace Relayer.Outbox.EntityFrameworkCore;

// Fixed stores select this at composition. Tenant stores select the native strategy in their delivery scope.
internal sealed class RetryingEntityFrameworkCoreConsumer<TContext>(EntityFrameworkCoreConsumer<TContext> boundary,
    string consumer, bool conflictRetries = false) : IConsumeTransactionMiddleware, IConsumeInbox where TContext : DbContext
{
    public bool Inbox => boundary.Inbox;

    private readonly EntityFrameworkCoreConsumeOperation?[] _operations = new EntityFrameworkCoreConsumeOperation[16];

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
    {
        var incoming = (DefaultMessageContext)context;
        var services = incoming.Services;
        var db = services.GetRequiredService<TContext>();
        var settings = boundary.Resolve(db);
        IExecutionStrategy strategy;
        bool useConflictStrategy = conflictRetries;

        if (boundary.ScopedValidation)
        {
            var native = db.Database.CreateExecutionStrategy();

            if (!native.RetriesOnFailure && !conflictRetries)
            {
                await boundary.InvokeAsync(context, next, db, settings).ConfigureAwait(false);
                return;
            }

            useConflictStrategy = conflictRetries && !native.RetriesOnFailure;
            strategy = useConflictStrategy ? settings.Configuration.CreateConflictStrategy(db) : native;
        }
        else
        {
            strategy = conflictRetries ? settings.Configuration.CreateConflictStrategy(db) : db.Database.CreateExecutionStrategy();
        }

        EntityFrameworkCoreConsumeOperation? operation = null;

        for (var i = 0; i < _operations.Length; i++)
        {
            if (Interlocked.Exchange(ref _operations[i], null) is { } cached)
            {
                operation = cached;
                break;
            }
        }

        operation ??= new EntityFrameworkCoreConsumeOperation();
        operation.Context = incoming;
        operation.Next = next;
        operation.Inbox = settings.Inbox;
        operation.Outbox = settings.Outbox;
        operation.VerifyCommit = settings.Inbox && !useConflictStrategy;

        try
        {
            await strategy.ExecuteAsync((Owner: this, Operation: operation),
                static (_, state, cancellation) => state.Owner.AttemptAsync(state.Operation, cancellation),
                operation.VerifyCommit ? static (_, state, cancellation) => state.Owner.VerifyAsync(state.Operation, cancellation) : null,
                context.CancellationToken).ConfigureAwait(false);
            // Cleanup belongs outside both native retries and handler-failure policy.
            if (incoming.ScopeCleanupError is { } cleanup)
            {
                ExceptionDispatchInfo.Throw(cleanup);
            }
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
            {
                if (Interlocked.CompareExchange(ref _operations[i], operation, null) is null)
                {
                    break;
                }
            }
        }
    }

    private async Task<bool> AttemptAsync(EntityFrameworkCoreConsumeOperation operation, CancellationToken cancellation)
    {
        var context = operation.Context;
        // A new native attempt means verification did not establish a committed result.
        context.SendBuffer.Clear();
        operation.CommitVerification = null;
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
                var settings = boundary.Resolve(db);

                if (settings.Inbox != operation.Inbox || settings.Outbox != operation.Outbox)
                {
                    throw new InvalidOperationException("The selected DbContext must keep the same inbox and outbox capabilities across native retry attempts.");
                }

                var scopedSender = context.Services.GetService<ISender>();

                if (scopedSender is not null && scopedSender is not Sender)
                {
                    throw new InvalidOperationException("EF Core consumer retries require Relayer's scoped ISender.");
                }

                sender = (Sender?)scopedSender;

                if (sender is not null)
                {
                    sender.AttemptBuffer = buffer;
                }

                await boundary.InvokeAsync(context, operation.Next, db, settings,
                    operation.VerifyCommit ? operation : null).ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    await scope.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    context.ScopeCleanupError = error;
                }
                finally
                {
                    sender?.EndAttempt();
                }
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

            if (operation.CommitVerification is null)
            {
                context.SendBuffer.Clear();
            }

            throw;
        }
        finally
        {
            context.SetServices(services);
        }
    }

    private async Task<ExecutionResult<bool>> VerifyAsync(EntityFrameworkCoreConsumeOperation operation, CancellationToken cancellation)
    {
        // EF can call verification for a transient failure before the transaction reached commit.
        if (operation.CommitVerification is not { } verification)
        {
            return new(false, true);
        }

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
                var settings = boundary.Resolve(db);

                if (settings.Inbox != operation.Inbox || settings.Outbox != operation.Outbox)
                {
                    throw new InvalidOperationException("The verification DbContext must keep the attempted inbox and outbox capabilities.");
                }

                committed = await verification.Configuration.InboxCommitVerifiedAsync(db.Database.GetDbConnection(),
                    consumer, verification.MessageId, verification.TransactionId, cancellation).ConfigureAwait(false);
                // Inbox rows commit only on the completion path.
                if (committed && context.Settlement.Request is null)
                {
                    context.Settlement.Complete();
                }
            }
            finally
            {
                try
                {
                    await scope.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    context.ScopeCleanupError = error;
                }
            }
        }
        catch when (context.ScopeCleanupError is not null)
        {
        }
        finally
        {
            context.SetServices(services);
        }

        return new(committed || context.ScopeCleanupError is not null, true);
    }
}

internal sealed class EntityFrameworkCoreConsumeOperation
{
    internal DefaultMessageContext Context = null!;
    internal ConsumeDelegate Next = null!;
    internal InboxCommitVerification? CommitVerification;
    internal bool Inbox;
    internal bool Outbox;
    internal bool VerifyCommit;

    internal void Clear()
    {
        Context = null!;
        Next = null!;
        CommitVerification = null;
    }
}

internal readonly record struct InboxCommitVerification(OutboxConfiguration Configuration, Guid MessageId, ulong TransactionId);
