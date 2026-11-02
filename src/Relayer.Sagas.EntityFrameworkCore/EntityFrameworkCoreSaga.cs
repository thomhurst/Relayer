using Microsoft.EntityFrameworkCore;

namespace Relayer.Sagas.EntityFrameworkCore;

/// <summary>Persists typed process transitions in the consumer's existing EF Core inbox/outbox transaction.</summary>
public abstract class EntityFrameworkCoreSaga<TContext, TState, TMessage>(TContext database) : Saga<TState, TMessage>
    where TContext : DbContext
    where TState : EntityFrameworkCoreSagaData
{
    /// <summary>The scoped context shared with the consumer boundary and outgoing sends.</summary>
    protected TContext Database { get; } = database ?? throw new ArgumentNullException(nameof(database));

    /// <inheritdoc />
    protected sealed override void ValidateTransaction()
    {
        if (Database.Database.CurrentTransaction is null)
            throw new InvalidOperationException("Saga handling requires the Saga consumer transaction boundary for the same scoped DbContext.");
    }

    /// <inheritdoc />
    protected sealed override ValueTask SaveAsync(TState state, bool isNew, CancellationToken cancellationToken)
    {
        if (isNew) Database.Add(state);
        else
        {
            var entry = Database.Entry(state);
            if (entry.State is not (EntityState.Unchanged or EntityState.Modified))
                throw new InvalidOperationException("LoadAsync must return tracked saga state from the selected scoped DbContext. Do not use AsNoTracking.");
            // Even a transition that only sends must participate in optimistic concurrency.
            entry.Property(nameof(SagaData.IsCompleted)).IsModified = true;
        }
        return new ValueTask(Database.SaveChangesAsync(cancellationToken));
    }
}
