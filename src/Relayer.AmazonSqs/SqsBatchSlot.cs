using System.Globalization;
using System.Threading.Tasks.Sources;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace Relayer.AmazonSqs;

// A worker advances to the next handler only after this delivery reaches settlement or exits.
// Both waits are retained per worker: no Task/continuation object is created per delivery.
internal sealed class SqsBatchSlot : IValueTaskSource, IValueTaskSource<bool>, IDisposable
{
    private ManualResetValueTaskSourceCore<bool> _ready;
    private ManualResetValueTaskSourceCore<bool> _deleted;
    internal readonly SqsDelivery Delivery;
    internal readonly DeleteMessageBatchRequestEntry Entry;
    internal ValueTask Processing;
    internal string? DeleteRequestId;
    internal bool PendingDelete { get; private set; }

    internal SqsBatchSlot(IAmazonSQS client, string url, TimeProvider time, int index)
    {
        Delivery = new(client, url, time) { BatchSlot = this };
        Entry = new() { Id = index.ToString(CultureInfo.InvariantCulture) };
    }

    internal void Reset()
    {
        _ready.Reset();
        _deleted.Reset();
        PendingDelete = false;
        DeleteRequestId = null;
    }

    internal ValueTask<bool> Ready => new(this, _ready.Version);
    internal void Finished()
    {
        // Pending deletes already signalled Ready before waiting for the native response.
        if (!PendingDelete) _ready.SetResult(true);
    }

    internal ValueTask DeleteAsync()
    {
        Entry.ReceiptHandle = Delivery.Message.ReceiptHandle;
        PendingDelete = true;
        _ready.SetResult(true);
        return new(this, _deleted.Version);
    }

    internal void CompleteDelete(Exception? error)
    {
        if (error is null) _deleted.SetResult(true);
        else _deleted.SetException(error);
    }

    public void Dispose() => Delivery.Dispose();
    void IValueTaskSource.GetResult(short token) => _deleted.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _deleted.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _deleted.OnCompleted(continuation, state, token, flags);
    bool IValueTaskSource<bool>.GetResult(short token) => _ready.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _ready.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _ready.OnCompleted(continuation, state, token, flags);
}
