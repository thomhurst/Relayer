using System.Threading.Tasks.Sources;

namespace Relayer.Kafka;

// One pump waiter, many completion producers. Notifications coalesce. No per-wait allocation.
internal sealed class KafkaPumpSignal : IValueTaskSource
{
    private ManualResetValueTaskSourceCore<bool> _completion = new() { RunContinuationsAsynchronously = true };
    // 0 empty, 1 waiter, 2 saved notification, 3 publishing a completion.
    private int _state;

    internal void Signal()
    {
        while (true)
        {
            var state = Volatile.Read(ref _state);
            if (state is 2 or 3) return;
            if (Interlocked.CompareExchange(ref _state, state == 0 ? 2 : 3, state) != state) continue;
            if (state == 0) return;
            try { _completion.SetResult(true); }
            finally { Volatile.Write(ref _state, 0); }
            return;
        }
    }

    internal ValueTask WaitAsync()
    {
        while (true)
        {
            if (Interlocked.CompareExchange(ref _state, 0, 2) == 2) return default;
            _completion.Reset();
            if (Interlocked.CompareExchange(ref _state, 1, 0) == 0) return new(this, _completion.Version);
        }
    }

    public void GetResult(short token)
    {
        // SetResult publishes the continuation before returning. Do not reset its source
        // while the winning producer is still finishing that publication.
        var spin = new SpinWait();
        while (Volatile.Read(ref _state) == 3) spin.SpinOnce();
        _completion.GetResult(token);
    }

    public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _completion.OnCompleted(continuation, state, token, flags);
}
