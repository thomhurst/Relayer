using System.Data.Common;

namespace Relayer;

// Source-linked into each SQL provider: state and cleanup are shared, native completion and ownership are not.
internal struct OutboxTransactionLifecycle
{
    internal bool Finished { get; private set; }
    private bool _completing;

    internal readonly bool CanUse => !Finished && !_completing;

    internal void BeginCompletion() => _completing = true;

    // Mark finished before notifying the owner or disposing native resources, including when either throws.
    internal bool TryFinish()
    {
        if (Finished) return false;
        Finished = true;
        return true;
    }

    internal static void DisposeNative<TTransaction, TConnection>(TTransaction transaction, TConnection? ownedConnection)
        where TTransaction : DbTransaction
        where TConnection : DbConnection
    {
        try { transaction.Dispose(); }
        finally { ownedConnection?.Dispose(); }
    }

    internal static async ValueTask DisposeNativeAsync<TTransaction, TConnection>(TTransaction transaction, TConnection? ownedConnection)
        where TTransaction : DbTransaction
        where TConnection : DbConnection
    {
        try { await transaction.DisposeAsync().ConfigureAwait(false); }
        finally
        {
            if (ownedConnection is not null) await ownedConnection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
