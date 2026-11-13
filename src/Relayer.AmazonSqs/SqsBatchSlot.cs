using System.Globalization;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Relayer.Transports;
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
    private readonly Completion _processed = new();
    private ConfiguredValueTaskAwaitable.ConfiguredValueTaskAwaiter _processing;
    private readonly Action _continueProcessing;
    private SqsConsumer? _owner;
    private CancellationToken _abort;
    private Activity? _deleteActivity;
    internal ValueTask<bool> Processing => _processed.Task;
    internal string? DeleteRequestId;
    internal bool PendingDelete { get; private set; }

    internal SqsBatchSlot(IAmazonSQS client, string url, TimeProvider time, int index)
    {
        _continueProcessing = FinishProcessing;
        Delivery = new(client, url, time) { BatchSlot = this };
        Entry = new() { Id = index.ToString(CultureInfo.InvariantCulture) };
    }

    internal void Reset()
    {
        _processed.Reset();
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

    internal void Process(SqsConsumer owner, CancellationToken abort)
    {
        _owner = owner;
        _abort = abort;
        try
        {
            _processing = owner.Receive(Delivery, abort).ConfigureAwait(false).GetAwaiter();
            if (!_processing.IsCompleted) { _processing.UnsafeOnCompleted(_continueProcessing); return; }
        }
        catch (Exception error) { FinishProcessing(error); return; }
        FinishProcessing();
    }

    private void FinishProcessing()
    {
        Exception? error = null;
        try { _processing.GetResult(); }
        catch (Exception caught) { error = caught; }
        FinishProcessing(error);
    }

    private void FinishProcessing(Exception? error)
    {
        if (error is not null && !(error is OperationCanceledException && _abort.IsCancellationRequested))
            _owner!.ReportProcessingFailure(error);
        Delivery.Clear();
        Finished();
        _owner = null;
        _processed.Complete();
    }

    internal ValueTask DeleteAsync(Activity? activity)
    {
        _deleteActivity = activity;
        Entry.ReceiptHandle = Delivery.Message.ReceiptHandle;
        PendingDelete = true;
        _ready.SetResult(true);
        return new(this, _deleted.Version);
    }

    internal void CompleteDelete(Exception? error)
    {
        var activity = _deleteActivity;
        _deleteActivity = null;
        activity?.SetTag("aws.request_id", DeleteRequestId ?? (error as AmazonSQSException)?.RequestId);
        if (error is not null) MessageOperationTracing.Fail(activity, (error as AmazonSQSException)?.ErrorCode ?? "_OTHER");
        activity?.Dispose();
        if (error is null) _deleted.SetResult(true);
        else _deleted.SetException(error);
    }

    private sealed class Completion : IValueTaskSource<bool>
    {
        private ManualResetValueTaskSourceCore<bool> _core;
        internal ValueTask<bool> Task => new(this, _core.Version);
        internal void Reset() => _core.Reset();
        internal void Complete() => _core.SetResult(true);
        public bool GetResult(short token) => _core.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);
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
