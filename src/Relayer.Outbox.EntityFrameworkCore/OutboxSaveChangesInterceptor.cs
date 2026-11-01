using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Relayer.Outbox.EntityFrameworkCore;

internal sealed class OutboxSaveChangesInterceptor(EntityFrameworkCoreOutbox outbox, OutboxConfiguration configuration)
    : SaveChangesInterceptor
{
    internal EntityFrameworkCoreOutbox Outbox { get; } = outbox;
    internal OutboxConfiguration Configuration { get; } = configuration;

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        var state = Outbox.Get(eventData.Context!, Configuration);
        state.CheckTransaction();
        state.EnsureTransactionAvailable();
        if (state.Count > 0 && state.Context.Database.CurrentTransaction is null)
        {
            try
            {
                ValidateExecutionStrategy(state.Context);
                state.OwnedTransaction = state.Context.Database.BeginTransaction();
            }
            catch { state.Clear(); throw; }
        }
        return result;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData,
        InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        var state = Outbox.Get(eventData.Context!, Configuration);
        state.CheckTransaction();
        state.EnsureTransactionAvailable();
        if (state.Count > 0 && state.Context.Database.CurrentTransaction is null)
        {
            try
            {
                ValidateExecutionStrategy(state.Context);
                state.OwnedTransaction = await state.Context.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            }
            catch { state.Clear(); throw; }
        }
        return result;
    }

    private static void ValidateExecutionStrategy(DbContext context)
    {
        if (context.Database.CreateExecutionStrategy().RetriesOnFailure)
            throw new InvalidOperationException("Relayer outbox requires an explicit transaction with a retrying EF Core execution strategy. " +
                "Wrap the complete unit in Database.CreateExecutionStrategy().Execute/ExecuteAsync, create a fresh scoped DbContext for each attempt, " +
                "and begin and commit an explicit transaction inside the strategy delegate.");
    }

    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        var state = Outbox.Get(eventData.Context!, Configuration);
        if (state.OwnedTransaction is { } transaction)
        {
            try { transaction.Commit(); }
            finally
            {
                transaction.Dispose();
                state.OwnedTransaction = null;
                state.Clear();
            }
        }
        return result;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public override async ValueTask<int> SavedChangesAsync(SaveChangesCompletedEventData eventData,
        int result, CancellationToken cancellationToken = default)
    {
        var state = Outbox.Get(eventData.Context!, Configuration);
        if (state.OwnedTransaction is { } transaction)
        {
            try { await transaction.CommitAsync(cancellationToken).ConfigureAwait(false); }
            finally
            {
                await transaction.DisposeAsync().ConfigureAwait(false);
                state.OwnedTransaction = null;
                state.Clear();
            }
        }
        return result;
    }

    public override void SaveChangesFailed(DbContextErrorEventData eventData) => Failed(eventData.Context!);
    public override Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken cancellationToken = default) =>
        FailedAsync(eventData.Context!);
    public override void SaveChangesCanceled(DbContextEventData eventData) => Failed(eventData.Context!);
    public override Task SaveChangesCanceledAsync(DbContextEventData eventData, CancellationToken cancellationToken = default) =>
        FailedAsync(eventData.Context!);

    private void Failed(DbContext context)
    {
        var state = Outbox.Get(context, Configuration);
        if (state.OwnedTransaction is { } transaction)
        {
            try { transaction.Dispose(); }
            finally { state.OwnedTransaction = null; state.Clear(); }
        }
        else if (state.Transaction is { } explicitTransaction) Outbox.Invalidate(explicitTransaction);
        else state.Clear();
    }

    private async Task FailedAsync(DbContext context)
    {
        var state = Outbox.Get(context, Configuration);
        if (state.OwnedTransaction is { } transaction)
        {
            try { await transaction.DisposeAsync().ConfigureAwait(false); }
            finally { state.OwnedTransaction = null; state.Clear(); }
        }
        else if (state.Transaction is { } explicitTransaction) Outbox.Invalidate(explicitTransaction);
        else state.Clear();
    }
}
