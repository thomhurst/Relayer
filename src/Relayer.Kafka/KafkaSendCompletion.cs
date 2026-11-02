using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Dekaf.Producer;

namespace Relayer.Kafka;

// Retained by a send state: pooled async builders can overflow their retention cap when
// an entire tombstone batch completes concurrently, allocating again on every batch.
internal sealed class KafkaSendCompletion : IValueTaskSource<RecordMetadata>
{
    private ManualResetValueTaskSourceCore<RecordMetadata> _source;
    private ConfiguredValueTaskAwaitable<RecordMetadata>.ConfiguredValueTaskAwaiter _pending;
    private SendTracing.NativeSendOperation _duration;
    private readonly Action _complete;

    internal KafkaSendCompletion() => _complete = Complete;

    internal ValueTask<RecordMetadata> Track(ValueTask<RecordMetadata> pending, SendTracing.NativeSendOperation duration)
    {
        _pending = pending.ConfigureAwait(false).GetAwaiter();
        _duration = duration;
        var version = _source.Version;
        if (_pending.IsCompleted) Complete();
        else _pending.UnsafeOnCompleted(_complete);
        return new(this, version);
    }

    private void Complete()
    {
        RecordMetadata result = default;
        Exception? failure = null;
        try { result = _pending.GetResult(); }
        catch (Exception error) { failure = error; }
        // Release the native source before publishing completion: the awaiting batch
        // can immediately return this state to its pool from an inline continuation.
        _pending = default;
        var duration = _duration;
        _duration = default;
        try { duration.Complete(failure is not null); }
        catch (Exception error) { failure = error; }
        if (failure is null) _source.SetResult(result);
        else _source.SetException(failure);
    }

    RecordMetadata IValueTaskSource<RecordMetadata>.GetResult(short token)
    {
        try { return _source.GetResult(token); }
        finally { _source.Reset(); }
    }

    ValueTaskSourceStatus IValueTaskSource<RecordMetadata>.GetStatus(short token) => _source.GetStatus(token);
    void IValueTaskSource<RecordMetadata>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _source.OnCompleted(continuation, state, token, flags);
}
