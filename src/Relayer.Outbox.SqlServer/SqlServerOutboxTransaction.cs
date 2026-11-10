using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer;

internal sealed class SqlServerOutboxTransaction : DbTransaction
{
    private readonly SqlServerOutbox _owner;
    private readonly SqlTransaction _native;
    private readonly SqlConnection? _ownedConnection;
    private readonly SqlServerOutboxConnection _connection;
    private readonly IsolationLevel _isolationLevel;
    internal bool ConsumerOwned { get; set; }
    private bool _finished;
    private bool _completing;

    internal SqlServerOutboxTransaction(SqlServerOutbox owner, SqlTransaction native, SqlConnection? ownedConnection)
    {
        _owner = owner;
        _native = native;
        _ownedConnection = ownedConnection;
        _connection = new(this, native.Connection!);
        _isolationLevel = native.IsolationLevel;
    }

    internal bool IsActive
    {
        get
        {
            if (_finished || _completing) return false;
            if (_native.Connection is null)
                throw new InvalidOperationException("The native SQL Server transaction ended outside Relayer. Dispose the outbox transaction and retry the complete unit of work.");
            return true;
        }
    }
    public override IsolationLevel IsolationLevel => _isolationLevel;
    protected override DbConnection? DbConnection => _finished ? null : _connection;

    internal SqlTransaction Unwrap(DbConnection? connection)
    {
        EnsureActive();
        if (!ReferenceEquals(connection, _connection))
            throw new InvalidOperationException("Use this outbox transaction's Connection to create business commands.");
        return _native;
    }

    private void EnsureActive()
    {
        if (!IsActive) throw new InvalidOperationException("The SQL Server outbox transaction has completed or is completing.");
    }

    private void EnsureCompletionAllowed()
    {
        if (ConsumerOwned) throw new InvalidOperationException("The SQL Server consumer owns transaction completion. Let Relayer commit or roll back after handling.");
    }

    public override void Commit()
    {
        EnsureCompletionAllowed();
        EnsureActive();
        _completing = true;
        try
        {
            _owner.Prepare(_native)?.Execute();
            _native.Commit();
            _owner.Committed();
        }
        finally { Finish(); }
    }

    public override async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureCompletionAllowed();
        EnsureActive();
        _completing = true;
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
        EnsureCompletionAllowed();
        EnsureActive();
        _completing = true;
        try { _native.Rollback(); }
        finally { Finish(); }
    }

    public override async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        EnsureCompletionAllowed();
        EnsureActive();
        _completing = true;
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
    private static NotSupportedException SavepointError() => new("SQL Server outbox transactions do not support savepoints. Roll back and retry the complete unit of work.");

    private void Finish()
    {
        if (_finished) return;
        _finished = true;
        _owner.Ended(this);
        try { _native.Dispose(); }
        finally { _ownedConnection?.Dispose(); }
    }

    private async ValueTask FinishAsync()
    {
        if (_finished) return;
        _finished = true;
        _owner.Ended(this);
        try { await _native.DisposeAsync().ConfigureAwait(false); }
        finally
        {
            if (_ownedConnection is not null) await _ownedConnection.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Finish();
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync() => FinishAsync();
}
