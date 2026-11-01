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
    internal Task Completion { get; private set; } = Task.CompletedTask;

    internal KafkaConsumer(IKafkaConsumer<byte[], byte[]> native,
        Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive, MessageTracing tracing, MessagingEndpoint endpoint,
        KafkaOwnership ownership)
    {
        _native = native;
        _receive = receive;
        _ownership = ownership;
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
        // Polling and processing never inherit the application's startup Activity.
        if (ExecutionContext.IsFlowSuppressed()) Completion = Task.Run(RunAsync);
        else { using (ExecutionContext.SuppressFlow()) Completion = Task.Run(RunAsync); }
    }

    private async Task RunAsync()
    {
        try
        {
            await foreach (var batch in _native.ConsumeBatchAsync(_stop.Token).ConfigureAwait(false))
                await ProcessBatchAsync(batch, _abort.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }

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
            var disposition = await _receive(new(record, partitionId), cancellationToken).ConfigureAwait(false);
            if (disposition.Kind is not (DispositionKind.Complete or DispositionKind.SettledByHandler))
                throw new InvalidOperationException("Kafka receive must complete durable failure handling before returning Complete. Kafka has no native redelivery or dead-letter settlement.");
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask CommitAsync(TopicPartitionOffset[] offsets, int count, CancellationToken cancellationToken)
    {
        using var activity = _commit.Start(new() { BatchCount = count });
        try { await _native.CommitAsync(offsets, cancellationToken).ConfigureAwait(false); }
        catch (Exception) { activity?.SetStatus(ActivityStatusCode.Error); activity?.SetTag("error.type", "_OTHER"); throw; }
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
        await _native.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
        _abort.Dispose();
    }
}

internal sealed class KafkaOwnership : IRebalanceListener
{
    private int _version;
    internal int Version => Volatile.Read(ref _version);
    public ValueTask OnPartitionsAssignedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
    { Interlocked.Increment(ref _version); return default; }
    public ValueTask OnPartitionsRevokedAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
    { Interlocked.Increment(ref _version); return default; }
    public ValueTask OnPartitionsLostAsync(IEnumerable<TopicPartition> partitions, CancellationToken cancellationToken)
    { Interlocked.Increment(ref _version); return default; }
}
