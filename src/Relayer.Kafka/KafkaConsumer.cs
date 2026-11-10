using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Dekaf;
using Dekaf.Consumer;

namespace Relayer.Kafka;

// One serial batch stream per declaration. Parallel partition scheduling belongs to #22.
internal sealed class KafkaConsumer : IAsyncDisposable
{
    private readonly IKafkaConsumer<byte[], byte[]> _native;
    private readonly Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> _receive;
    private readonly MessageOperationTracing _commit;
    private readonly Dictionary<int, string> _partitions = [];
    private readonly TopicPartitionOffset[] _offset = new TopicPartitionOffset[1];
    private CancellationTokenSource _stop = new();
    private CancellationTokenSource _abort = new();
    private readonly KafkaOwnership _ownership;
    private readonly KafkaWorkerHealth? _health;
    private readonly KafkaReplay _replay = new();
    private ConsumerCircuitDelivery? _circuit;

    internal ConsumerCircuitDelivery ConfigureCircuit(ConsumerCircuitOptions options, EndpointTelemetry telemetry, TimeProvider time)
    {
        _circuit = new(options, telemetry, _replay.Wake, time);
        _ownership.Assigned = partitions =>
        {
            if (_circuit.State == 1) _native.Pause(partitions.ToArray());
        };
        return _circuit;
    }
    internal string Topic { get; }
    internal string? Group { get; }
    internal Task Completion { get; private set; } = Task.CompletedTask;

    internal KafkaConsumer(IKafkaConsumer<byte[], byte[]> native,
        Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive, MessageTracing tracing, MessagingEndpoint endpoint,
        KafkaOwnership ownership, KafkaWorkerHealth? health = null)
    {
        _native = native;
        _receive = receive;
        _ownership = ownership;
        _health = health;
        Topic = endpoint.Destination;
        Group = endpoint.ConsumerGroup;
        _commit = new(tracing, endpoint, MessagingOperation.Settle, "commit");
    }

    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!Completion.IsCompleted) throw new InvalidOperationException("The Kafka consumer is already running.");
        if (Completion.IsFaulted || Completion.IsCanceled || _abort.IsCancellationRequested)
            throw new InvalidOperationException("Recreate the application host after a failed or aborted Kafka receive. A new native consumer resumes at the broker's committed offset.");
        if (_stop.IsCancellationRequested)
        {
            _stop.Dispose();
            _abort.Dispose();
            _stop = new();
            _abort = new();
        }
        await _native.InitializeAsync(cancellationToken).ConfigureAwait(false);
        _health?.Idle();
        _replay.Start();
        // Polling and processing never inherit the application's startup Activity.
        if (ExecutionContext.IsFlowSuppressed()) Completion = Task.Run(RunAsync);
        else { using (ExecutionContext.SuppressFlow()) Completion = Task.Run(RunAsync); }
    }

    private async Task RunAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                using var poll = _replay.BeginPolling(_stop.Token);
                try
                {
                    var batches = _native.ConsumeBatchAsync(poll.Token).GetAsyncEnumerator();
                    await using var lease = batches.ConfigureAwait(false);
                    while (true)
                    {
                        // Only polling cancellation means a control wake-up. A handler or
                        // commit cancellation must still fail the receiver and its batch.
                        try
                        {
                            if (!await batches.MoveNextAsync().ConfigureAwait(false)) break;
                        }
                        catch (OperationCanceledException) when (poll.IsCancellationRequested) { break; }
                        _health?.Progress();
                        await ProcessBatchAsync(batches.Current, _abort.Token).ConfigureAwait(false);
                        _health?.Idle();
                    }
                }
                finally { _replay.EndPolling(); }
                _circuit?.RecordState();
                if (_circuit is { State: 2 }) _native.Resume(_native.Assignment.ToArray());
                // The native stream has released its borrowed batch before any seek.
                // Neither polling nor old checkpoint commits can overlap control.
                while (_replay.TryRead(out var request))
                {
                    try
                    {
                        await ApplySeekAsync(request).ConfigureAwait(false);
                        request.Completion.TrySetResult();
                    }
                    catch (OperationCanceledException) { request.Completion.TrySetCanceled(request.Token); }
                    catch (Exception error) { request.Completion.TrySetException(error); }
                }
                if (!poll.IsCancellationRequested) break;
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        finally { _replay.Stop(); _health?.Unavailable(); }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<int> ProcessBatchAsync(ConsumeBatch<byte[], byte[]> batch, CancellationToken cancellationToken)
    {
        var version = _ownership.Version;
        var partition = batch.TopicPartition;
        if (!_partitions.TryGetValue(partition.Partition, out var partitionId))
            _partitions.Add(partition.Partition, partitionId = partition.Partition.ToString(CultureInfo.InvariantCulture));
        var count = 0;
        var complete = true;
        var records = batch.GetEnumerator();
        if (!records.MoveNext()) return 0;
        var rewind = new TopicPartitionOffset(partition.Topic, partition.Partition, records.Current.Offset);
        do
        {
            if (version != _ownership.Version || !_native.Assignment.Contains(partition))
            { complete = false; break; }
            var record = records.Current;
            if (_circuit is { } circuit && !circuit.TryEnter())
            {
                Hold(new(partition.Topic, partition.Partition, record.Offset));
                return 0;
            }
            var disposition = await _receive(new(record, partitionId), cancellationToken).ConfigureAwait(false);
            if (_circuit is { Retained: true })
            {
                Hold(new(partition.Topic, partition.Partition, record.Offset));
                return 0;
            }
            if (_circuit is { State: 0 } && _native.Paused.Count != 0)
                _native.Resume(_native.Assignment.ToArray());
            if (disposition.Kind is not (DispositionKind.Complete or DispositionKind.SettledByHandler))
                throw new InvalidOperationException("Kafka receive must complete durable failure handling before returning Complete. Kafka has no native redelivery or dead-letter settlement.");
            _health?.Progress();
            count++;
        }
        while (records.MoveNext());
        // A rebalance invalidates the entire borrowed batch. Never acknowledge its old
        // offsets under a new ownership epoch, even when the same partition was reassigned.
        if (!complete || version != _ownership.Version)
        {
            // A retained partition keeps its native fetch position. Replay the whole
            // uncommitted window before a later batch can acknowledge beyond it.
            if (_native.Assignment.Contains(partition)) _native.Seek(rewind);
            return 0;
        }
        if (!batch.TryGetNextOffset(out var checkpoint)) return 0;
        _offset[0] = checkpoint;
        await CommitAsync(_offset, count, cancellationToken).ConfigureAwait(false);
        return count;
    }

    private void Hold(TopicPartitionOffset offset)
    {
        _native.Pause(_native.Assignment.ToArray());
        if (_native.Assignment.Contains(new TopicPartition(offset.Topic, offset.Partition))) _native.Seek(offset);
        _replay.Wake();
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask CommitAsync(TopicPartitionOffset[] offsets, int count, CancellationToken cancellationToken)
    {
        using var activity = _commit.Start(new() { BatchCount = count });
        try { await _native.CommitAsync(offsets, cancellationToken).ConfigureAwait(false); }
        catch (Exception) { activity?.SetStatus(ActivityStatusCode.Error); activity?.SetTag("error.type", "_OTHER"); throw; }
    }

    internal async ValueTask SeekAsync(TopicPartitionOffset offset, TopicPartitionTimestamp? timestamp, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset.Partition);
        if (timestamp is null) ArgumentOutOfRangeException.ThrowIfNegative(offset.Offset);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureRunning();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stop.Token);
        var request = new KafkaSeekRequest(offset, timestamp, stop.Token);
        _replay.Enqueue(request);
        await request.Completion.Task.WaitAsync(stop.Token).ConfigureAwait(false);
    }

    private async ValueTask ApplySeekAsync(KafkaSeekRequest request)
    {
        var cancellationToken = request.Token;
        cancellationToken.ThrowIfCancellationRequested();
        var offset = request.Offset;
        var partition = new TopicPartition(offset.Topic, offset.Partition);
        var ownership = _ownership.Version;
        EnsureAssigned(partition);
        if (request.Timestamp is { } search)
        {
            var offsets = await _native.GetOffsetsForTimesAsync([search], cancellationToken).ConfigureAwait(false);
            if (!offsets.TryGetValue(partition, out var found) || found < 0)
                throw new InvalidOperationException($"Kafka has no matching timestamp offset for '{partition}'. No seek was applied.");
            offset = new(offset.Topic, offset.Partition, found);
        }
        cancellationToken.ThrowIfCancellationRequested();
        if (ownership != _ownership.Version)
            throw new InvalidOperationException("Kafka assignment changed during seek. Retry against the current assignment.");
        EnsureAssigned(partition);
        _native.Seek(offset);
    }

    private void EnsureRunning()
    {
        if (_stop.IsCancellationRequested || Completion.IsCompleted)
            throw new InvalidOperationException("Kafka consumer is not running. Seek only after startup and before shutdown.");
    }

    private void EnsureAssigned(TopicPartition partition)
    {
        if (!_native.Assignment.Contains(partition))
            throw new InvalidOperationException($"Kafka partition '{partition}' is not assigned to this consumer. Replay must target its current local owner.");
    }

    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        _stop.Cancel();
        using var registration = cancellationToken.UnsafeRegister(static state => ((CancellationTokenSource)state!).Cancel(), _abort);
        try { await Completion.WaitAsync(cancellationToken).ConfigureAwait(false); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _abort.Cancel();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _abort.Cancel();
        try { await Completion.ConfigureAwait(false); }
        catch (Exception) when (Completion.IsFaulted || Completion.IsCanceled) { /* Hosted service reports failures. */ }
        _circuit?.Dispose();
        await _native.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
        _abort.Dispose();
    }
}

internal sealed class KafkaOwnership : IRebalanceListener
{
    private int _version;
    internal Action<IEnumerable<TopicPartition>>? Assigned { get; set; }
    internal int Version => Volatile.Read(ref _version);
    public ValueTask OnPartitionsAssignedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
    { Interlocked.Increment(ref _version); Assigned?.Invoke(partitions); return default; }
    public ValueTask OnPartitionsRevokedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
    { Interlocked.Increment(ref _version); return default; }
    public ValueTask OnPartitionsLostAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
    { Interlocked.Increment(ref _version); return default; }
}
