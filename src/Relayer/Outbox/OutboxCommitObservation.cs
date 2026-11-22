using System.Data.Common;
using Relayer.Pipeline;

namespace Relayer.Outbox;

// Register before native commit makes the rows visible to recovery. Keep unsettled rows
// after a successful commit and withdraw provisional work if the transaction fails.
internal sealed class OutboxCommitObservation : IDisposable
{
    private readonly IRelayerObserver _observer;
    private readonly Guid[] _rows;
    private bool _finished;

    internal OutboxCommitObservation(IRelayerObserver observer, Guid[] rows)
    {
        _observer = observer;
        _rows = rows;
        observer.OutboxCommitting(rows);
    }

    internal void Commit()
    {
        _finished = true;
    }

    internal async Task CommitAsync(DbTransaction transaction, CancellationToken cancellationToken)
    {
        using (this)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            Commit();
        }
    }

    public void Dispose()
    {
        if (_finished)
        {
            return;
        }

        _finished = true;

        foreach (var row in _rows)
        {
            _observer.OutboxSettled(row);
        }
    }
}
