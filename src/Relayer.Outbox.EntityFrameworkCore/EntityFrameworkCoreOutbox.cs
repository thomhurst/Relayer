using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

// Scoped, like DbContext and ISender. Never shared by concurrent units of work.
internal sealed class EntityFrameworkCoreOutbox : IOutboxEnlistment, IDisposable
{
    private readonly List<OutboxContext> _contexts = [];
    private readonly PostgreSqlOutboxBatch _batch = new();
    private Selection? _selection;

    public bool IsActive => Target() is not null;

    public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var target = Target() ?? throw new InvalidOperationException("Select an EF Core outbox context or begin an EF Core transaction before sending.");
        target.Add(record);
        return ValueTask.CompletedTask;
    }

    internal OutboxContext Get(DbContext context, OutboxConfiguration configuration)
    {
        foreach (var state in _contexts)
            if (ReferenceEquals(state.Context, context)) return state;
        if (context.Database.ProviderName != "Npgsql.EntityFrameworkCore.PostgreSQL")
            throw new InvalidOperationException("EF Core outbox enlistment currently requires the Npgsql PostgreSQL provider.");
        var created = new OutboxContext(context, configuration);
        _contexts.Add(created);
        return created;
    }

    internal IDisposable Select(DbContext context, OutboxConfiguration configuration)
    {
        var state = Get(context, configuration);
        state.CheckTransaction();
        state.EnsureTransactionAvailable();
        var selection = new Selection(this, state, _selection);
        state.SelectionDepth++;
        _selection = selection;
        return selection;
    }

    private OutboxContext? Target()
    {
        if (_selection is { State: var selected })
        {
            selected.CheckTransaction();
            selected.EnsureTransactionAvailable();
            return selected;
        }
        OutboxContext? target = null;
        foreach (var state in _contexts)
        {
            state.CheckTransaction();
            if (state.Transaction is null || !state.HasCurrentTransaction()) continue;
            if (target is not null)
                throw new InvalidOperationException("Several DbContexts have active transactions. Use context.EnlistRelayerOutbox() to select the owner of sends.");
            target = state;
        }
        return target;
    }

    internal void Started(DbContext context, OutboxConfiguration configuration, DbTransaction transaction, bool newTransaction)
    {
        // Npgsql can reuse the same native object after disposal. A new EF transaction is a new unit.
        if (newTransaction) Ended(transaction);
        var state = Get(context, configuration);
        state.CheckTransaction();
        if (state.Transaction is not null && !ReferenceEquals(state.Transaction, transaction))
            throw new InvalidOperationException("This DbContext still has buffered work in a live shared transaction. Complete that transaction before reusing the context.");
        state.Transaction = transaction;
        state.Detached = false;
    }

    internal void Ended(DbTransaction transaction)
    {
        foreach (var state in _contexts)
            if (ReferenceEquals(state.Transaction, transaction)) state.Clear();
    }

    internal void Committed(DbTransaction transaction)
    {
        foreach (var state in _contexts)
        {
            if (!ReferenceEquals(state.Transaction, transaction)) continue;
            try { state.Configuration.Dispatcher?.Committed(state.Rows, state.Count); }
            finally { state.Clear(); }
        }
    }

    internal void Invalidate(DbTransaction transaction)
    {
        foreach (var state in _contexts)
            if (ReferenceEquals(state.Transaction, transaction)) state.Failed = true;
    }

    internal PostgreSqlOutboxBatch? Prepare(DbTransaction transaction)
    {
        OutboxConfiguration? configuration = null;
        var count = 0;
        foreach (var state in _contexts)
        {
            if (!ReferenceEquals(state.Transaction, transaction)) continue;
            if (state.Failed) throw new InvalidOperationException("This outbox unit failed. Roll back the transaction and retry the complete unit of work.");
            if (configuration is not null && (configuration.Table != state.Configuration.Table || configuration.Lanes != state.Configuration.Lanes))
                throw new InvalidOperationException("DbContexts sharing a transaction must use identical outbox schema, prefix and lane settings.");
            configuration = state.Configuration;
            count = checked(count + state.Count);
        }
        if (count == 0) return null;
        _batch.Prepare(transaction, configuration!.InsertSql);
        foreach (var state in _contexts)
            if (ReferenceEquals(state.Transaction, transaction))
                for (var i = 0; i < state.Count; i++) _batch.Add(state.Rows[i]);
        return _batch;
    }

    public void Dispose()
    {
        foreach (var state in _contexts) state.Clear();
        _batch.Dispose();
    }

    private sealed class Selection(EntityFrameworkCoreOutbox owner, OutboxContext state, Selection? previous) : IDisposable
    {
        internal OutboxContext State { get; } = state;
        private bool _disposed;
        public void Dispose()
        {
            if (_disposed) return;
            if (!ReferenceEquals(owner._selection, this))
                throw new InvalidOperationException("Dispose EF Core outbox selections in reverse creation order.");
            owner._selection = previous;
            State.SelectionDepth--;
            // An explicit transaction keeps ownership until commit/rollback; an unsaved implicit unit is abandoned.
            State.CheckTransaction();
            if (State.SelectionDepth == 0 && State.Transaction is null) State.Clear();
            _disposed = true;
        }
    }
}

internal sealed class OutboxContext(DbContext context, OutboxConfiguration configuration)
{
    internal DbContext Context { get; } = context;
    internal OutboxConfiguration Configuration { get; } = configuration;
    internal DbTransaction? Transaction { get; set; }
    internal IDbContextTransaction? OwnedTransaction { get; set; }
    internal bool Failed { get; set; }
    internal bool Detached { get; set; }
    internal List<BufferedOutboxMessage> Rows { get; } = [];
    internal int Count { get; private set; }
    internal int SelectionDepth { get; set; }

    internal void CheckTransaction()
    {
        if (Transaction is null) return;
        // Disposing a borrowed EF wrapper does not end the native shared transaction.
        // Keep its rows until native completion, even when this context no longer participates.
        try
        {
            if (Transaction.Connection is null) Clear();
        }
        catch (ObjectDisposedException) { Clear(); }
    }

    internal bool HasCurrentTransaction()
    {
        if (Detached) return false;
        try
        {
            if (ReferenceEquals(Context.Database.CurrentTransaction?.GetDbTransaction(), Transaction)) return true;
        }
        catch (ObjectDisposedException) { }
        Detached = true;
        return false;
    }

    internal void EnsureTransactionAvailable()
    {
        if (Transaction is not null && !HasCurrentTransaction())
            throw new InvalidOperationException("This DbContext no longer participates in its live shared transaction. Complete that transaction before reusing the context.");
    }

    internal void Add(OutboxRecord record)
    {
        if (Failed) throw new InvalidOperationException("Roll back the failed outbox transaction before sending again.");
        BufferedOutboxMessage.Validate(record);
        if (Count == Rows.Count) Rows.Add(new());
        Rows[Count].Copy(record, Configuration.Lanes);
        Count++;
    }

    internal void Clear()
    {
        for (var i = 0; i < Count; i++) Rows[i].Clear();
        Count = 0;
        Transaction = null;
        Failed = false;
        Detached = false;
    }
}
