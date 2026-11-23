using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class OutboxTransactionInterceptor(EntityFrameworkCoreOutbox outbox, OutboxConfiguration configuration)
    : DbTransactionInterceptor
{
    public override InterceptionResult<DbTransaction> TransactionStarting(DbConnection connection,
        TransactionStartingEventData eventData, InterceptionResult<DbTransaction> result)
    {
        configuration.TenantBinding?.Validate();
        return result;
    }

    public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(DbConnection connection,
        TransactionStartingEventData eventData, InterceptionResult<DbTransaction> result, CancellationToken cancellationToken = default)
    {
        return new(TransactionStarting(connection, eventData, result));
    }

    public override DbTransaction TransactionStarted(DbConnection connection, TransactionEndEventData eventData, DbTransaction result)
    {
        outbox.Started(eventData.Context!, configuration, result, newTransaction: true);
        return result;
    }

    public override ValueTask<DbTransaction> TransactionStartedAsync(DbConnection connection, TransactionEndEventData eventData,
        DbTransaction result, CancellationToken cancellationToken = default)
    {
        return new(TransactionStarted(connection, eventData, result));
    }

    public override DbTransaction TransactionUsed(DbConnection connection, TransactionEventData eventData, DbTransaction result)
    {
        outbox.Started(eventData.Context!, configuration, result, newTransaction: false);
        return result;
    }

    public override ValueTask<DbTransaction> TransactionUsedAsync(DbConnection connection, TransactionEventData eventData,
        DbTransaction result, CancellationToken cancellationToken = default)
    {
        return new(TransactionUsed(connection, eventData, result));
    }

    public override InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        try
        {
            outbox.Prepare(transaction, result.IsSuppressed)?.Execute();
            outbox.ObserveCommit(transaction);
        }
        catch
        {
            outbox.Invalidate(transaction);
            throw;
        }

        return result;
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public override async ValueTask<InterceptionResult> TransactionCommittingAsync(DbTransaction transaction, TransactionEventData eventData,
        InterceptionResult result, CancellationToken cancellationToken = default)
    {
        try
        {
            if (outbox.Prepare(transaction, result.IsSuppressed) is { } batch)
            {
                await batch.ExecuteAsync(cancellationToken).ConfigureAwait(false);
                outbox.ObserveCommit(transaction);
            }
        }
        catch
        {
            outbox.Invalidate(transaction);
            throw;
        }

        return result;
    }

    public override void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData)
    {
        outbox.Committed(transaction);
    }

    public override Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        outbox.Committed(transaction);
        return Task.CompletedTask;
    }

    public override void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData)
    {
        outbox.Ended(transaction);
    }

    public override Task TransactionRolledBackAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        outbox.Ended(transaction);
        return Task.CompletedTask;
    }

    public override void TransactionFailed(DbTransaction transaction, TransactionErrorEventData eventData)
    {
        if (eventData.Action == "Commit" && configuration.HasTransactionEnded(transaction))
        {
            outbox.Ended(transaction);
            return;
        }

        // EF creates its automatic savepoint before any save command. When that fails, for example on a
        // canceled token, nothing from this save reached the database and the unit can continue.
        if (eventData.Context is { } context && outbox.Get(context, configuration).SavepointFailed())
        {
            return;
        }

        outbox.Invalidate(transaction);
    }

    public override Task TransactionFailedAsync(DbTransaction transaction, TransactionErrorEventData eventData, CancellationToken cancellationToken = default)
    {
        TransactionFailed(transaction, eventData);
        return Task.CompletedTask;
    }

    public override InterceptionResult CreatingSavepoint(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        var state = outbox.Get(eventData.Context!, configuration);

        if (state.Saving)
        {
            state.CreatingSavepoint = true;
        }

        return result;
    }

    public override ValueTask<InterceptionResult> CreatingSavepointAsync(DbTransaction transaction, TransactionEventData eventData,
        InterceptionResult result, CancellationToken cancellationToken = default)
    {
        return new(CreatingSavepoint(transaction, eventData, result));
    }

    public override void CreatedSavepoint(DbTransaction transaction, TransactionEventData eventData)
    {
        var state = outbox.Get(eventData.Context!, configuration);

        if (state.Saving)
        {
            state.SavepointCreated = true;
        }
    }

    public override Task CreatedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        CreatedSavepoint(transaction, eventData);
        return Task.CompletedTask;
    }

    public override void ReleasedSavepoint(DbTransaction transaction, TransactionEventData eventData)
    {
        var state = outbox.Get(eventData.Context!, configuration);
        state.CreatingSavepoint = false;
        state.SavepointCreated = false;
    }

    public override Task ReleasedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        ReleasedSavepoint(transaction, eventData);
        return Task.CompletedTask;
    }

    public override void RolledBackToSavepoint(DbTransaction transaction, TransactionEventData eventData)
    {
        // Manual rewinds, and saves without a confirmed native checkpoint, remain fatal.
        if (!outbox.Get(eventData.Context!, configuration).RollbackSave())
        {
            outbox.Invalidate(transaction);
        }
    }

    public override Task RolledBackToSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
    {
        RolledBackToSavepoint(transaction, eventData);
        return Task.CompletedTask;
    }
}
