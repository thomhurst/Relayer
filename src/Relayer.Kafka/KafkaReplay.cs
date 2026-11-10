using System.Diagnostics.CodeAnalysis;
using Dekaf;

namespace Relayer.Kafka;

internal sealed class KafkaSeekRequest(TopicPartitionOffset offset, TopicPartitionTimestamp? timestamp, CancellationToken token)
{
    internal TopicPartitionOffset Offset { get; } = offset;
    internal TopicPartitionTimestamp? Timestamp { get; } = timestamp;
    internal CancellationToken Token { get; } = token;
    internal TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

// Only operational requests and poll-stream boundaries take this lock. Processing
// messages and fetching successive native batches never touch the command queue.
internal sealed class KafkaReplay
{
    private readonly object _gate = new();
    private readonly Queue<KafkaSeekRequest> _requests = new();
    private CancellationTokenSource? _poll;
    private bool _running;
    private bool _wake;

    internal void Wake()
    {
        lock (_gate) { _wake = true; _poll?.Cancel(); }
    }

    internal void Start()
    {
        lock (_gate) _running = true;
    }

    internal void Enqueue(KafkaSeekRequest request)
    {
        lock (_gate)
        {
            if (!_running) throw new InvalidOperationException("Kafka consumer is not running.");
            _requests.Enqueue(request);
            _poll?.Cancel();
        }
    }

    internal CancellationTokenSource BeginPolling(CancellationToken stop)
    {
        lock (_gate)
        {
            _poll = CancellationTokenSource.CreateLinkedTokenSource(stop);
            if (_requests.Count != 0 || _wake) _poll.Cancel();
            _wake = false;
            return _poll;
        }
    }

    internal void EndPolling()
    {
        lock (_gate) _poll = null;
    }

    internal bool TryRead([NotNullWhen(true)] out KafkaSeekRequest? request)
    {
        lock (_gate) return _requests.TryDequeue(out request);
    }

    internal void Stop()
    {
        lock (_gate)
        {
            _running = false;
            while (_requests.TryDequeue(out var request))
                request.Completion.TrySetException(new InvalidOperationException("Kafka consumer stopped before applying the seek."));
        }
    }
}
