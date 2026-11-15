using Relayer.Pipeline;
using Relayer.Transports;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Dekaf;
using Dekaf.Consumer;

namespace Relayer.Kafka;

// Wraps Dekaf's partitioned runtime (D22): native key-ordered lanes, contiguous completion tracking,
// runtime-managed commits, rebalance drain, backpressure, seek control and outstanding-record monitoring.
internal sealed class KafkaConsumer : IAsyncDisposable
{
    private static readonly Func<int, string> PartitionText = static partition => partition.ToString(CultureInfo.InvariantCulture);
    private readonly Func<IKafkaConsumer<byte[], byte[]>> _create;
    private IKafkaConsumer<byte[], byte[]> _native;
    private bool _ran;
    private readonly Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> _receive;
    private readonly PartitionRecordProcessor<byte[], byte[]> _process;
    private readonly PartitionedProcessingOptions _options;
    private readonly PartitionedProcessingControl _control = new();
    private readonly PartitionedProcessingMonitor? _monitor;
    private readonly ConcurrentDictionary<int, string> _partitions = new();
    private readonly KafkaWorkerHealth? _health;
    private CancellationTokenSource _stop = new();
    private ConsumerCircuit? _circuit;
    private EndpointTelemetry? _circuitTelemetry;
    // Completed and replaced on every circuit transition. A held record captures it before each attempt.
    private TaskCompletionSource _circuitChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Benchmarks run the composed native processor directly.
    internal PartitionedProcessingOptions Options => _options;
    internal PartitionRecordProcessor<byte[], byte[]> Processor => _process;
    internal string Topic { get; }
    internal string? Group { get; }
    internal Task Completion { get; private set; } = Task.CompletedTask;

    // Each start runs a new native consumer: a stopped run discards records it fetched but did not process, so
    // only a fresh member resumes at the committed offset without skipping them.
    internal KafkaConsumer(Func<IKafkaConsumer<byte[], byte[]>> create,
        Func<KafkaDelivery, CancellationToken, ValueTask<Disposition>> receive, MessageTracing tracing, MessagingEndpoint endpoint,
        PartitionedProcessingOptions options, int concurrency, KafkaWorkerHealth? health = null)
    {
        _create = create;
        _native = create();
        _receive = receive;
        _health = health;
        _process = ProcessAsync;
        Topic = endpoint.Destination;
        Group = endpoint.ConsumerGroup;
        _monitor = health is null ? null : new PartitionedProcessingMonitor();
        _options = KafkaProcessing.Compose(options, concurrency, _control, _monitor,
            new KafkaCommitObserver(new(tracing, endpoint, MessagingOperation.Settle, "commit")));
    }

    // Uses the concurrent core policy: rejected and circuit-opening deliveries return Redeliver and stay
    // unmarked, so native completion cannot pass them. Native backpressure pauses their partitions.
    internal void ConfigureCircuit(ConsumerCircuitOptions options, ConsumePipeline pipeline, EndpointTelemetry telemetry, TimeProvider time)
    {
        _circuitTelemetry = telemetry;
        _circuit = new(options.FailureRatio, options.MinimumThroughput, options.SamplingDuration,
            options.BreakDuration, time, CircuitChanged);
        pipeline.Circuit = _circuit;
        pipeline.CircuitChanged = _ => CircuitChanged();
        // Kafka has no native delivery limit, so permanent failures dead-letter and count as successful outcomes.
        pipeline.CircuitCountsRetryFailuresOnly = true;
        telemetry.RecordCircuitState(0);
    }

    private void CircuitChanged()
    {
        _circuitTelemetry?.RecordCircuitState(_circuit!.State);
        Interlocked.Exchange(ref _circuitChanged, new(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
    }

    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!Completion.IsCompleted)
            throw new InvalidOperationException(_stop.IsCancellationRequested
                ? "The Kafka consumer is still draining after a stop. Wait for the stop to finish before starting it again."
                : "The Kafka consumer is already running.");
        if (Completion.IsFaulted || Completion.IsCanceled)
            throw new InvalidOperationException("Recreate the application host after a failed Kafka receive. A new native consumer resumes at the broker's committed offset.");
        if (_stop.IsCancellationRequested)
        {
            _stop.Dispose();
            _stop = new();
        }
        if (_ran) _native = _create();
        _ran = true;
        try { await _native.InitializeAsync(cancellationToken).ConfigureAwait(false); }
        catch
        {
            await _native.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        if (_health is not null)
        {
            var monitor = _monitor!;
            _health.Start(() => monitor.OldestOutstandingRecordAt);
        }
        // Polling and processing never inherit the application's startup Activity.
        if (ExecutionContext.IsFlowSuppressed()) Completion = Task.Run(RunAsync);
        else { using (ExecutionContext.SuppressFlow()) Completion = Task.Run(RunAsync); }
    }

    private async Task RunAsync()
    {
        var native = _native;
        try { await native.RunPartitionedAsync(_process, _options, _stop.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        finally
        {
            _health?.Unavailable();
            // Leave the group once the drain has committed, so the stopped member's partitions move to the others.
            await native.DisposeAsync().ConfigureAwait(false);
        }
    }

    // Returning marks the record processed for native contiguous commits, so return only after durable handling.
    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ProcessAsync(PartitionRecordProcessorContext<byte[], byte[]> context,
        ConsumeResult<byte[], byte[]> record, CancellationToken cancellationToken)
    {
        var delivery = new KafkaDelivery(record, _partitions.GetOrAdd(record.Partition, PartitionText));
        while (true)
        {
            var changed = _circuit is null ? null : Volatile.Read(ref _circuitChanged);
            var disposition = await _receive(delivery, cancellationToken).ConfigureAwait(false);
            if (disposition.Kind is DispositionKind.Complete or DispositionKind.SettledByHandler) return;
            if (changed is null || disposition.Kind != DispositionKind.Redeliver)
                throw new InvalidOperationException("Kafka receive must complete durable failure handling before returning Complete. Kafka has no native redelivery or dead-letter settlement.");
            // Hold this record in its key lane until the circuit admits another attempt. When the lane starts stopping
            // (revoke, seek or shutdown), the native stop-requested token leaves it unprocessed for replay.
            await changed.Task.WaitAsync(context.StopRequestedToken).ConfigureAwait(false);
        }
    }

    internal async ValueTask SeekAsync(TopicPartitionOffset offset, TopicPartitionTimestamp? timestamp, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset.Partition);
        if (timestamp is null) ArgumentOutOfRangeException.ThrowIfNegative(offset.Offset);
        cancellationToken.ThrowIfCancellationRequested();
        if (_stop.IsCancellationRequested || Completion.IsCompleted)
            throw new InvalidOperationException("Kafka consumer is not running. Seek only after startup and before shutdown.");
        if (timestamp is { } search)
        {
            // Dekaf's timestamp seek moves to the partition end when nothing matches. Replay must not
            // silently skip records, so resolve with the same native lookup and fail instead.
            var partition = search.TopicPartition;
            var offsets = await _native.GetOffsetsForTimesAsync([search], cancellationToken).ConfigureAwait(false);
            if (!offsets.TryGetValue(partition, out var found) || found < 0)
                throw new InvalidOperationException($"Kafka has no matching timestamp offset for '{partition}'. No seek was applied.");
            offset = new(partition.Topic, partition.Partition, found);
        }
        await _control.SeekAsync(offset, cancellationToken).ConfigureAwait(false);
    }

    // Graceful stop lets the native runtime drain lanes within StopTimeout and commit completed offsets.
    // Caller cancellation only stops waiting; the drain continues and a later stop can await it.
    internal Task StopAsync(CancellationToken cancellationToken)
    {
        _stop.Cancel();
        return Completion.WaitAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        try { await Completion.ConfigureAwait(false); }
        catch (Exception) when (Completion.IsFaulted || Completion.IsCanceled) { /* Hosted service reports failures. */ }
        _circuit?.Dispose();
        await _native.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
    }
}

// Reports each runtime-managed commit as one amortized CLIENT settle span (design §8.2).
internal sealed class KafkaCommitObserver(MessageOperationTracing commit) : IOffsetCommitObserver
{
    public object? OnCommitStarting(OffsetCommitObservation observation)
    {
        var count = observation.CompletedRecordCount;
        // Commits are ambient work on the runtime loop; never leave the span current for lanes it starts.
        var previous = Activity.Current;
        var activity = commit.Start(new() { BatchCount = count > 0 ? (int)Math.Min(count, int.MaxValue) : null });
        Activity.Current = previous;
        return activity;
    }

    public void OnCommitCompleted(OffsetCommitObservation observation, object? state, Exception? exception)
    {
        if (state is not Activity activity) return;
        if (exception is not null)
        {
            activity.SetStatus(ActivityStatusCode.Error);
            activity.SetTag("error.type", "_OTHER");
        }
        var current = Activity.Current;
        activity.Dispose();
        Activity.Current = current;
    }
}

internal static class KafkaProcessing
{
    // Relayer's defaults when the declaration supplies no native options.
    internal static PartitionedProcessingOptions Defaults(int concurrency) => new()
    {
        Ordering = PartitionedProcessingOrder.Key,
        MaxConcurrentHandlersPerPartition = concurrency,
        CommitPolicy = PartitionCommitPolicy.CommitCompletedPeriodically,
    };

    internal static void Validate(string source, PartitionedProcessingOptions options, int concurrency)
    {
        if (options.MaxConcurrentHandlers is { } handlers && handlers != concurrency)
            throw Invalid($"Processing.MaxConcurrentHandlers ({handlers}) must be unset or equal Concurrency({concurrency}); Concurrency sets the native consumer-wide limit.");
        if (options.CommitPolicy == PartitionCommitPolicy.UserManaged)
            throw Invalid("Processing.CommitPolicy must be runtime-managed. Relayer commits only offsets the native runtime has seen complete.");
        if (options.ErrorPolicy != PartitionWorkerErrorPolicy.StopConsumer)
            throw Invalid("Processing.ErrorPolicy must be StopConsumer. Other policies skip or stall records after failure handling itself fails.");
        if (options.Monitor is not null || options.Control is not null || options.CommitObserver is not null)
            throw Invalid("leave Processing.Monitor, Control and CommitObserver unset. Relayer supplies them for health, replay and commit telemetry.");
        if (options.MaxHandlerBatchSize != 1)
            throw Invalid("Processing.MaxHandlerBatchSize must be 1. Kafka batch handlers are not implemented yet.");

        InvalidOperationException Invalid(string reason) => new($"Kafka consumer '{source}': {reason}");
    }

    // PartitionedProcessingOptions is init-only, so pass every native setting through on a new instance.
    internal static PartitionedProcessingOptions Compose(PartitionedProcessingOptions options, int concurrency,
        PartitionedProcessingControl control, PartitionedProcessingMonitor? monitor, IOffsetCommitObserver observer) => new()
    {
        MaxBufferedRecordsPerPartition = options.MaxBufferedRecordsPerPartition,
        Ordering = options.Ordering,
        MaxConcurrentHandlersPerPartition = options.MaxConcurrentHandlersPerPartition,
        MaxConcurrentHandlers = concurrency,
        MaxBufferedRecords = options.MaxBufferedRecords,
        MaxHandlerBatchSize = options.MaxHandlerBatchSize,
        BackpressureMode = options.BackpressureMode,
        StopPolicy = options.StopPolicy,
        StopTimeout = options.StopTimeout,
        ErrorPolicy = options.ErrorPolicy,
        IgnoreRestartBackoff = options.IgnoreRestartBackoff,
        IgnoreRestartBackoffMax = options.IgnoreRestartBackoffMax,
        CommitPolicy = options.CommitPolicy,
        CommitInterval = options.CommitInterval,
        Monitor = monitor,
        Control = control,
        CommitObserver = observer,
    };
}
