using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer.Outbox.EntityFrameworkCore;

// Scoped, like DbContext and ISender. Never shared by concurrent units of work.
internal sealed class EntityFrameworkCoreOutbox(bool supportsScheduling = false, bool supportsOrderedScheduling = false,
    RelayerInstrumentation? instrumentation = null) : IOutboxEnlistment, IDisposable
{
    private readonly List<OutboxContext> _contexts = [];
    private IOutboxBatch? _batch;
    private Selection? _selection;
    private long _enqueueOrder;
    private readonly List<BufferedOutboxMessage> _sharedRows = [];
    private static readonly Comparison<BufferedOutboxMessage> s_sendOrder = static (left, right) => left.EnqueueOrder.CompareTo(right.EnqueueOrder);

    private OutboxContext? _activeContext;

    public bool SupportsScheduling => _activeContext?.SupportsScheduling ?? supportsScheduling;

    public bool SupportsOrderedScheduling => _activeContext?.SupportsOrderedScheduling ?? supportsOrderedScheduling;

    public bool IsActive
    {
        get
        {
            // Sender already resolves the active target. Keep it for scheduling checks without another context scan.
            _activeContext = Target();
            return _activeContext is not null;
        }
    }

    public ValueTask EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var target = Target() ?? throw new InvalidOperationException("Select an EF Core outbox context or begin an EF Core transaction before sending.");
        target.Add(record, checked(++_enqueueOrder));
        return ValueTask.CompletedTask;
    }

    internal OutboxContext Get(DbContext context, OutboxConfiguration configuration)
    {
        foreach (var state in _contexts)
        {
            if (ReferenceEquals(state.Context, context))
            {
                return state;
            }
        }

        configuration.ValidateWriter(context);
        var created = new OutboxContext(context, configuration);
        _contexts.Add(created);
        return created;
    }

    internal IOutboxSelection Select(DbContext context, OutboxConfiguration configuration, IServiceProvider services)
    {
        configuration.TenantBinding?.Validate();
        var state = Get(context, configuration);
        state.CheckTransaction();
        state.EnsureTransactionAvailable();
        var selection = new Selection(this, state, _selection, services);
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

            if (state.Transaction is null || !state.HasCurrentTransaction())
            {
                continue;
            }

            if (target is not null)
            {
                throw new InvalidOperationException("Several DbContexts have active transactions. Use context.EnlistRelayerOutbox() to select the owner of sends.");
            }

            target = state;
        }

        return target;
    }

    internal void Started(DbContext context, OutboxConfiguration configuration, DbTransaction transaction, bool newTransaction)
    {
        // Npgsql can reuse the same native object after disposal. A new EF transaction is a new unit.
        if (newTransaction)
        {
            Ended(transaction);
        }

        configuration.TenantBinding?.Validate();
        var state = Get(context, configuration);
        state.CheckTransaction();

        if (state.Transaction is not null && !ReferenceEquals(state.Transaction, transaction))
        {
            throw new InvalidOperationException("This DbContext still has buffered work in a live shared transaction. Complete that transaction before reusing the context.");
        }

        state.Transaction = transaction;
        state.Detached = false;
    }

    internal void Ended(DbTransaction transaction)
    {
        foreach (var state in _contexts)
        {
            if (ReferenceEquals(state.Transaction, transaction))
            {
                instrumentation?.FinishOutboxCommit(state, committed: false);
                state.Clear();
            }
        }
    }

    internal void ObserveCommit(DbTransaction transaction)
    {
        if (instrumentation?.Observer is null)
        {
            return;
        }

        foreach (var state in _contexts)
        {
            if (ReferenceEquals(state.Transaction, transaction))
            {
                instrumentation.ObserveOutboxCommit(state, state.Rows, state.Count);
            }
        }
    }

    internal void Committed(DbTransaction transaction)
    {
        foreach (var state in _contexts)
        {
            if (!ReferenceEquals(state.Transaction, transaction))
            {
                continue;
            }

            try
            {
                instrumentation?.FinishOutboxCommit(state, committed: true);
                instrumentation?.AddOutboxPending(state.Count);
                state.Configuration.Committed(state.Rows, state.Count);
            }
            finally
            {
                state.Clear();
            }
        }
    }

    internal void Invalidate(DbTransaction transaction)
    {
        foreach (var state in _contexts)
        {
            if (ReferenceEquals(state.Transaction, transaction))
            {
                state.Failed = true;
                instrumentation?.FinishOutboxCommit(state, committed: false);
            }
        }
    }

    internal IOutboxBatch? Prepare(DbTransaction transaction, bool commitSuppressed)
    {
        OutboxConfiguration? configuration = null;
        var count = 0;
        var participants = 0;

        foreach (var state in _contexts)
        {
            if (!ReferenceEquals(state.Transaction, transaction))
            {
                continue;
            }

            state.Configuration.TenantBinding?.Validate();

            if (state.Failed)
            {
                throw new InvalidOperationException("This outbox unit failed. Roll back the transaction and retry the complete unit of work.");
            }

            if (configuration is not null && (configuration.Table != state.Configuration.Table || configuration.Lanes != state.Configuration.Lanes ||
                configuration.TenantBinding?.TenantId != state.Configuration.TenantBinding?.TenantId))
            {
                throw new InvalidOperationException("DbContexts sharing a transaction must use identical outbox schema, prefix, lane and authorized tenant settings.");
            }

            configuration = state.Configuration;
            count = checked(count + state.Count);

            if (state.Count != 0)
            {
                state.Configuration.ValidateTransaction(transaction);
                state.ValidateScheduledCommit(transaction);
                participants++;
            }
        }

        if (commitSuppressed)
        {
            if (count != 0)
            {
                throw new InvalidOperationException("Suppressing a transaction commit with buffered Relayer outbox messages is unsupported. Roll back the transaction and allow EF Core to commit when retrying the unit of work.");
            }

            Ended(transaction);
            return null;
        }

        if (count == 0)
        {
            return null;
        }

        _batch ??= configuration!.CreateBatch();
        _batch.Prepare(transaction, configuration!.InsertSql);
        // A single context already buffers in send order. Shared transactions can interleave
        // sends across contexts; merge their reusable rows before the database assigns seq.
        foreach (var state in _contexts)
        {
            if (ReferenceEquals(state.Transaction, transaction))
            {
                if (participants == 1)
                {
                    _batch.AddRange(CollectionsMarshal.AsSpan(state.Rows)[..state.Count]);
                }
                else
                {
                    for (var i = 0; i < state.Count; i++)
                    {
                        _sharedRows.Add(state.Rows[i]);
                    }
                }
            }
        }

        if (participants > 1)
        {
            try
            {
                SortSendOrder(CollectionsMarshal.AsSpan(_sharedRows));
                _batch.AddRange(CollectionsMarshal.AsSpan(_sharedRows));
            }
            finally
            {
                _sharedRows.Clear();
            }
        }

        return _batch;
    }

    internal static void SortSendOrder(Span<BufferedOutboxMessage> rows)
    {
        rows.Sort(s_sendOrder);
    }

    public void Dispose()
    {
        foreach (var state in _contexts)
        {
            instrumentation?.FinishOutboxCommit(state, committed: false);
            state.Clear();
        }

        _batch?.Dispose();
    }

    private sealed class Selection(EntityFrameworkCoreOutbox owner, OutboxContext state, Selection? previous, IServiceProvider services) : IOutboxSelection, IOutboxEnlistment
    {
        internal OutboxContext State
        {
            get;
        } = state;
        private bool _disposed;
        private SendPipeline? _pipeline;
        private TenantContext? _tenant;

        public ValueTask EnqueueAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        {
            return Enqueue(message, null, cancellationToken);
        }

        public ValueTask EnqueueAsync<TMessage>(TMessage message, Action<SendOptions> configure, CancellationToken cancellationToken = default)
        {
            return Enqueue(message, configure ?? throw new ArgumentNullException(nameof(configure)), cancellationToken);
        }

        private ValueTask Enqueue<TMessage>(TMessage message, Action<SendOptions>? configure, CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            ArgumentNullException.ThrowIfNull(message);

            if (cancellationToken.IsCancellationRequested)
            {
                return ValueTask.FromCanceled(cancellationToken);
            }

            if (_pipeline is null)
            {
                _tenant = services.GetService<TenantContext>();
                _pipeline = services.GetRequiredService<SendPipeline>();
            }

            return _pipeline.GetTarget<TMessage>().SendAsync(message, configure, this, cancellationToken, null, tenant: _tenant?.Id ?? default);
        }

        bool IOutboxEnlistment.IsActive => !_disposed;

        bool IOutboxEnlistment.SupportsScheduling => State.SupportsScheduling;

        bool IOutboxEnlistment.SupportsOrderedScheduling => State.SupportsOrderedScheduling;

        ValueTask IOutboxEnlistment.EnlistAsync(OutboxRecord record, CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();
            State.CheckTransaction();
            State.EnsureTransactionAvailable();
            State.Add(record, checked(++owner._enqueueOrder));
            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            if (!ReferenceEquals(owner._selection, this))
            {
                throw new InvalidOperationException("Dispose EF Core outbox selections in reverse creation order.");
            }

            owner._selection = previous;
            State.SelectionDepth--;
            _disposed = true;

            if (State.SelectionDepth == 0 && State.OwnedTransaction is { } unfinished)
            {
                State.DisposeOwnedTransaction(unfinished);
                return;
            }

            State.CheckTransaction();

            if (State.SelectionDepth == 0 && State.Transaction is null)
            {
                State.Clear();
            }
        }
    }
}

internal sealed class OutboxContext(DbContext context, OutboxConfiguration configuration)
{
    private const string SchedulingError = "Scheduled EF Core outbox sends require a matching due-row dispatch worker for the selected DbContext database, schema, table prefix and ordered lane count. Register matching maintenance or ordered dispatch for that store.";
    private (bool Unordered, bool Ordered) _scheduled;
    private (bool Unordered, bool Ordered) _savedScheduled;

    internal DbContext Context
    {
        get;
    } = context;

    internal OutboxConfiguration Configuration
    {
        get;
    } = configuration;

    internal bool SupportsScheduling => GetScheduling().Unordered;

    internal bool SupportsOrderedScheduling => GetScheduling().Ordered;

    internal DbTransaction? Transaction
    {
        get; set;
    }

    internal IDbContextTransaction? OwnedTransaction
    {
        get; set;
    }

    internal bool Failed
    {
        get; set;
    }

    internal bool Detached
    {
        get; set;
    }

    internal List<BufferedOutboxMessage> Rows
    {
        get;
    } = [];

    internal int Count
    {
        get; private set;
    }

    internal int SelectionDepth
    {
        get; set;
    }
    private int _savedCount;

    internal bool Saving
    {
        get; private set;
    }

    internal bool CreatingSavepoint
    {
        get; set;
    }

    internal bool SavepointCreated
    {
        get; set;
    }

    internal bool SaveRecovered
    {
        get; private set;
    }

    internal void BeginSave()
    {
        Saving = true;
        CreatingSavepoint = false;
        SavepointCreated = false;
        SaveRecovered = false;
    }

    internal void DisposeOwnedTransaction(IDbContextTransaction transaction)
    {
        try
        {
            transaction.Dispose();
        }
        finally
        {
            Clear();
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask DisposeOwnedTransactionAsync(IDbContextTransaction transaction)
    {
        try
        {
            await transaction.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            Clear();
        }
    }

    internal void Saved()
    {
        _savedCount = Count;
        _savedScheduled = _scheduled;
        Saving = false;
        CreatingSavepoint = false;
        SavepointCreated = false;
        SaveRecovered = false;
    }

    internal bool RollbackSave()
    {
        return SavepointCreated && Recover();
    }

    // A failed automatic savepoint creation precedes every save command, so the database is unchanged by this save.
    internal bool SavepointFailed()
    {
        return CreatingSavepoint && !SavepointCreated && Recover();
    }

    private bool Recover()
    {
        if (!Saving || Failed)
        {
            return false;
        }

        DiscardUnsaved();
        SaveRecovered = true;
        return true;
    }

    internal void DiscardUnsaved()
    {
        // Sends made before SavingChanges belong to this save too. The last successful
        // save, rather than native savepoint creation, establishes the buffer boundary.
        for (var i = _savedCount; i < Count; i++)
        {
            Rows[i].Clear();
        }

        Count = _savedCount;
        _scheduled = _savedScheduled;
    }

    internal bool FinishFailedSave()
    {
        var recovered = Saving && SaveRecovered && !Failed;
        Saving = false;
        CreatingSavepoint = false;
        SavepointCreated = false;
        SaveRecovered = false;
        return recovered;
    }

    internal void CheckTransaction()
    {
        if (Transaction is null)
        {
            return;
        }
        // Disposing a borrowed EF wrapper does not end the native shared transaction.
        // Keep its rows until native completion, even when this context no longer participates.
        try
        {
            if (Transaction.Connection is null)
            {
                Clear();
            }
        }
        catch (ObjectDisposedException)
        {
            Clear();
        }
    }

    internal bool HasCurrentTransaction()
    {
        if (Detached)
        {
            return false;
        }

        try
        {
            if (ReferenceEquals(Context.Database.CurrentTransaction?.GetDbTransaction(), Transaction))
            {
                return true;
            }
        }
        catch (ObjectDisposedException)
        {
        }

        Detached = true;
        return false;
    }

    internal void EnsureTransactionAvailable()
    {
        if (Transaction is not null && !HasCurrentTransaction())
        {
            throw new InvalidOperationException("This DbContext no longer participates in its live shared transaction. Complete that transaction before reusing the context.");
        }
    }

    private (bool Unordered, bool Ordered) GetScheduling()
    {
        return Configuration.GetScheduling(Transaction?.Connection ?? Context.Database.GetDbConnection());
    }

    internal void ValidateScheduledCommit(DbTransaction transaction)
    {
        if (_scheduled == default)
        {
            return;
        }

        // A pending selection can buffer sends before EF opens the committing connection.
        var (unordered, ordered) = Configuration.GetScheduling(transaction.Connection!);

        if ((_scheduled.Unordered && !unordered) || (_scheduled.Ordered && !ordered))
        {
            throw new InvalidOperationException(SchedulingError);
        }
    }

    internal void Add(OutboxRecord record, long enqueueOrder)
    {
        if (Failed)
        {
            throw new InvalidOperationException("Roll back the failed outbox transaction before sending again.");
        }

        var scheduled = record.Message.DeliverAt is not null;

        if (scheduled && !(record.Message.OrderingKey is not null ? SupportsOrderedScheduling : SupportsScheduling))
        {
            throw new InvalidOperationException(SchedulingError);
        }

        if (Configuration.TenantBinding is { } tenant)
        {
            try
            {
                record = tenant.Bind(record);
            }
            catch
            {
                Failed = true;
                throw;
            }
        }

        BufferedOutboxMessage.Validate(record);

        if (Count == Rows.Count)
        {
            Rows.Add(new());
        }

        Rows[Count].Copy(record, Configuration.Lanes);
        Rows[Count].EnqueueOrder = enqueueOrder;
        Count++;

        if (scheduled)
        {
            if (record.Message.OrderingKey is null)
            {
                _scheduled.Unordered = true;
            }
            else
            {
                _scheduled.Ordered = true;
            }
        }
    }

    internal void Clear()
    {
        for (var i = 0; i < Count; i++)
        {
            Rows[i].Clear();
        }

        Count = 0;
        _savedCount = 0;
        _scheduled = default;
        _savedScheduled = default;
        Saving = false;
        CreatingSavepoint = false;
        SavepointCreated = false;
        SaveRecovered = false;
        Transaction = null;
        OwnedTransaction = null;
        Failed = false;
        Detached = false;
    }
}
