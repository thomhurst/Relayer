using System.Data;
using System.Data.Common;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

internal sealed class MySqlOutboxTransaction : DbTransaction
{
    private readonly MySqlOutbox _owner;
    private readonly MySqlTransaction _native;
    private readonly MySqlConnection? _ownedConnection;
    private readonly MySqlOutboxConnection _connection;
    private readonly IsolationLevel _isolationLevel;
    private OutboxTransactionLifecycle _lifecycle;

    internal MySqlOutboxTransaction(MySqlOutbox owner, MySqlTransaction native, MySqlConnection? ownedConnection)
    {
        _owner = owner;
        _native = native;
        _ownedConnection = ownedConnection;
        _connection = new(this, native.Connection!);
        _isolationLevel = native.IsolationLevel;
    }

    internal bool ConsumerOwned { get; set; }

    private void EnsureCallerOwnership()
    {
        if (ConsumerOwned) throw new InvalidOperationException("Relayer owns the MySQL consumer transaction. Return from the handler or request retry/dead-letter instead of completing it.");
    }

    internal Task CommitConsumerAsync(CancellationToken cancellationToken) => CommitCoreAsync(cancellationToken);

    internal bool IsActive
    {
        get
        {
            if (!_lifecycle.CanUse) return false;
            if (_native.Connection is null)
                throw new InvalidOperationException("The native MySQL transaction ended outside Relayer. Dispose the outbox transaction and retry the complete unit of work.");
            return true;
        }
    }
    public override IsolationLevel IsolationLevel => _isolationLevel;
    protected override DbConnection? DbConnection => _lifecycle.Finished ? null : _connection;

    internal MySqlTransaction Unwrap(DbConnection? connection)
    {
        EnsureActive();
        if (!ReferenceEquals(connection, _connection))
            throw new InvalidOperationException("Use this outbox transaction's Connection to create business commands.");
        return _native;
    }

    private void EnsureActive()
    {
        if (!IsActive) throw new InvalidOperationException("The MySQL outbox transaction has completed or is completing.");
    }

    public override void Commit()
    {
        EnsureCallerOwnership();
        EnsureActive();
        _lifecycle.BeginCompletion();
        try
        {
            _owner.Prepare(_native)?.Execute();
            _native.Commit();
            _owner.Committed();
        }
        finally { Finish(); }
    }

    public override Task CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureCallerOwnership();
        return CommitCoreAsync(cancellationToken);
    }

    private async Task CommitCoreAsync(CancellationToken cancellationToken)
    {
        EnsureActive();
        _lifecycle.BeginCompletion();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_owner.Prepare(_native) is { } batch)
                await batch.ExecuteAsync(cancellationToken).ConfigureAwait(false);
            await _native.CommitAsync(cancellationToken).ConfigureAwait(false);
            _owner.Committed();
        }
        finally { await FinishAsync().ConfigureAwait(false); }
    }

    public override void Rollback()
    {
        EnsureCallerOwnership();
        EnsureActive();
        _lifecycle.BeginCompletion();
        try { _native.Rollback(); }
        finally { Finish(); }
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        EnsureCallerOwnership();
        EnsureActive();
        _lifecycle.BeginCompletion();
        try { await _native.RollbackAsync(cancellationToken).ConfigureAwait(false); }
        finally { await FinishAsync().ConfigureAwait(false); }
    }

    // Savepoints would also need buffer checkpoints. Reject them rather than silently commit discarded business work's messages.
    public override bool SupportsSavepoints => false;
    public override void Save(string savepointName) => throw SavepointError();
    public override void Rollback(string savepointName) => throw SavepointError();
    public override void Release(string savepointName) => throw SavepointError();
    public override Task SaveAsync(string savepointName, CancellationToken cancellationToken = default) => throw SavepointError();
    public override Task RollbackAsync(string savepointName, CancellationToken cancellationToken = default) => throw SavepointError();
    public override Task ReleaseAsync(string savepointName, CancellationToken cancellationToken = default) => throw SavepointError();
    private static NotSupportedException SavepointError() => new("MySQL outbox transactions do not support savepoints. Roll back and retry the complete unit of work.");

    private void Finish()
    {
        if (!_lifecycle.TryFinish()) return;
        _owner.Ended(this);
        OutboxTransactionLifecycle.DisposeNative(_native, _ownedConnection);
    }

    private ValueTask FinishAsync()
    {
        if (!_lifecycle.TryFinish()) return ValueTask.CompletedTask;
        _owner.Ended(this);
        return OutboxTransactionLifecycle.DisposeNativeAsync(_native, _ownedConnection);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Finish();
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync() => FinishAsync();
}
