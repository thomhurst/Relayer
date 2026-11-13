using Relayer.Pipeline;
using System.Threading.Channels;
using Relayer.Transports;

namespace Relayer.InMemory;

internal sealed class InMemoryEndpoint
{
    private readonly string _name;
    private readonly InMemoryMessage?[] _pool;
    private readonly Channel<InMemoryMessage>[] _partitions;
    private readonly Channel<InMemoryMessage>[] _scheduled;
    private readonly ScheduleWake[] _scheduleWake;
    private readonly PriorityQueue<InMemoryMessage, (DateTimeOffset Due, long Sequence)>[] _due;
    private readonly long[] _nextScheduledSequence;
    private readonly Channel<byte> _sendSlots;
    private readonly Dictionary<string, Channel<InMemoryMessage>[]> _groups = new(StringComparer.Ordinal);
    private readonly List<InMemoryReceiver> _receivers = [];
    private readonly FanOut[] _fanOut;
    private readonly int _capacity;
    private readonly InMemoryMessage _wakeup = new(true);
    private readonly InMemoryReadState? _queueReader;
    private int _nextPartition = -1;
    internal TimeProvider Clock { get; }
    internal InMemorySettlementTracing? SettlementTracing { get; }
    internal TransportCapabilities Capabilities { get; }
    internal Channel<InMemoryDeadLetter> DeadLetters { get; } = Channel.CreateUnbounded<InMemoryDeadLetter>();

    internal InMemoryEndpoint(string name, InMemoryEndpointOptions options, TimeProvider clock, MessageTracing? tracing = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Capabilities);
        _name = name;
        Clock = clock;
        SettlementTracing = tracing is null ? null : new(tracing, name);
        Capabilities = options.Capabilities;
        if (options.Capacity < 1 || options.Partitions < 1 || options.MessagePoolCapacity < 1)
            throw new ArgumentException($"Endpoint '{name}': Capacity, Partitions and MessagePoolCapacity must be positive.", nameof(options));
        if (!Enum.IsDefined(Capabilities.ConsumptionModel) || Capabilities.MaxBatchSize < 1 ||
            Capabilities.MaxDeliveryDelay < TimeSpan.Zero || Capabilities.MaxRedeliveryDelay < TimeSpan.Zero || Capabilities.MaxMessageBytes <= 0)
            throw new ArgumentException($"Endpoint '{name}': invalid TransportCapabilities values.", nameof(options));
        if (Capabilities.Replay || Capabilities.Transactions || Capabilities.PauseResume)
            throw new NotSupportedException($"Endpoint '{name}': in-memory Replay, Transactions and PauseResume are not implemented. Disable these capabilities.");
        if (Capabilities.ConsumptionModel == ConsumptionModel.Queue && !Capabilities.OrderedByKey && options.Partitions != 1)
            throw new ArgumentException($"Endpoint '{name}': unordered queues require Partitions = 1. Enable OrderedByKey or use ConsumptionModel.Log.", nameof(options));
        _capacity = options.Capacity;
        _pool = new InMemoryMessage[options.MessagePoolCapacity];
        for (var i = 0; i < _pool.Length; i++) _pool[i] = new();
        var sharedQueue = Capabilities.ConsumptionModel == ConsumptionModel.Queue && !Capabilities.OrderedByKey;
        _partitions = CreatePartitions(options.Partitions, sharedQueue);
        if (sharedQueue) _queueReader = new(_partitions[0], _wakeup, shared: true);
        _fanOut = new FanOut[options.Partitions];
        for (var i = 0; i < _fanOut.Length; i++) _fanOut[i] = new(new(_partitions[i], _wakeup));
        var slots = checked(_capacity * options.Partitions);
        _scheduled = new Channel<InMemoryMessage>[options.Partitions];
        _scheduleWake = new ScheduleWake[options.Partitions];
        for (var i = 0; i < _scheduled.Length; i++)
        {
            // Global admission bounds all data, even when every send chooses this partition.
            // Reserve one additional slot for a coalesced wake-up, including while stopped.
            _scheduled[i] = Channel.CreateBounded<InMemoryMessage>(new BoundedChannelOptions(checked(slots + 1))
            {
                SingleReader = true,
                AllowSynchronousContinuations = false,
            });
            _scheduleWake[i] = new(_scheduled[i].Writer, _wakeup);
        }
        _due = new PriorityQueue<InMemoryMessage, (DateTimeOffset, long)>[options.Partitions];
        _nextScheduledSequence = new long[options.Partitions];
        for (var i = 0; i < _due.Length; i++) _due[i] = new();
        _sendSlots = Channel.CreateBounded<byte>(slots);
        for (var i = 0; i < slots; i++) _sendSlots.Writer.TryWrite(0);
    }

    private Channel<InMemoryMessage> CreateChannel(bool wakeSlot) => Channel.CreateBounded<InMemoryMessage>(new BoundedChannelOptions(checked(_capacity + (wakeSlot ? 1 : 0)))
    {
        FullMode = BoundedChannelFullMode.Wait,
        AllowSynchronousContinuations = false,
    });

    private Channel<InMemoryMessage>[] CreatePartitions(int count, bool wakeSlot = false)
    {
        var result = new Channel<InMemoryMessage>[count];
        for (var i = 0; i < count; i++) result[i] = CreateChannel(wakeSlot);
        return result;
    }

    internal void Register(InMemoryReceiveDelegate receive, string? group, int concurrency)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(concurrency, 1);
        var ordered = Capabilities.OrderedByKey || Capabilities.ConsumptionModel == ConsumptionModel.Log;
        if (ordered && concurrency != _partitions.Length)
            throw new InvalidOperationException($"Endpoint '{_name}': ordered delivery requires Concurrency({_partitions.Length}), one receiver per partition.");
        Channel<InMemoryMessage>[] channels;
        if (Capabilities.ConsumptionModel == ConsumptionModel.Log)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(group);
            channels = CreatePartitions(_partitions.Length);
            if (!_groups.TryAdd(group, channels))
                throw new InvalidOperationException($"Endpoint '{_name}': group '{group}' already has receivers. Configure it in one consumer declaration.");
        }
        else
        {
            if (group is not null) throw new InvalidOperationException($"Endpoint '{_name}': Group is available only for ConsumptionModel.Log.");
            if (ordered && _receivers.Count != 0)
                throw new InvalidOperationException($"Endpoint '{_name}': declare one ordered queue consumer to preserve key order.");
            channels = _partitions;
        }
        for (var i = 0; i < concurrency; i++)
            _receivers.Add(new(this, _queueReader ?? new(channels[ordered ? i : 0], _wakeup), receive, group, ordered ? i : 0));
    }

    internal long GetCommittedPosition(string group, int partition)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(partition, _partitions.Length);
        foreach (var receiver in _receivers)
            if (receiver.Group == group && receiver.Partition == partition) return receiver.CommittedPosition;
        throw new InvalidOperationException($"Endpoint '{_name}': unknown group '{group}'.");
    }

    internal ValueTask<byte> ReserveAsync(CancellationToken cancellationToken) => _sendSlots.Reader.ReadAsync(cancellationToken);
    internal IRelayerObserver? Observer { get; set; }

    internal InMemoryMessage Rent()
    {
        Observer?.WorkStarted();
        for (var i = 0; i < _pool.Length; i++)
            if (Interlocked.Exchange(ref _pool[i], null) is { } message) { message.References = 1; return message; }
        return new() { References = 1 };
    }

    internal void Release(InMemoryMessage message)
    {
        if (Interlocked.Decrement(ref message.References) != 0) return;
        message.Clear();
        for (var i = 0; i < _pool.Length; i++)
            if (Interlocked.CompareExchange(ref _pool[i], message, null) is null) break;
        _sendSlots.Writer.TryWrite(0);
        Observer?.WorkCompleted();
    }

    internal void ValidateSend(InMemoryMessage message)
    {
        if (Capabilities.MaxMessageBytes is { } maximum && message.Size > maximum)
            throw new InvalidOperationException($"Endpoint '{_name}': native body, headers and key exceed MaxMessageBytes ({maximum}).");
        if (message.OrderingKey is not null && !Capabilities.OrderedByKey)
            throw new InvalidOperationException($"Endpoint '{_name}': OrderedByKey is unavailable.");
        if (message.DeliverAt is { } due && due > Clock.GetUtcNow() &&
            (Capabilities.MaxDeliveryDelay is not { } limit || due - Clock.GetUtcNow() > limit))
            throw new InvalidOperationException($"Endpoint '{_name}': DeliverAt exceeds MaxDeliveryDelay or native scheduling is unavailable.");
        message.Partition = message.OrderingKey is { } key ? (int)(Hash(key) % (uint)_partitions.Length)
            : (int)((uint)Interlocked.Increment(ref _nextPartition) % (uint)_partitions.Length);
    }

    private static uint Hash(WireString key)
    {
        var hash = 2166136261u;
        // Normalize both representations without allocating a decoded string.
        if (key.Text is { } text)
        {
            foreach (var character in text.EnumerateRunes()) hash = unchecked((hash ^ (uint)character.Value) * 16777619);
        }
        else
        {
            var utf8 = key.Utf8.Span;
            while (!utf8.IsEmpty)
            {
                System.Text.Rune.DecodeFromUtf8(utf8, out var character, out var consumed);
                hash = unchecked((hash ^ (uint)character.Value) * 16777619);
                utf8 = utf8[Math.Max(consumed, 1)..];
            }
        }
        return hash;
    }

    internal ValueTask EnqueueAsync(InMemoryMessage message, CancellationToken cancellationToken) =>
        message.DeliverAt is { } due && due > Clock.GetUtcNow()
            ? _scheduled[message.Partition].Writer.WriteAsync(message, cancellationToken)
            : _partitions[message.Partition].Writer.WriteAsync(message, cancellationToken);

    internal void Start(List<Task> tasks, CancellationToken stop, CancellationToken abort)
    {
        _queueReader?.Prepare();
        // A full partition must not suspend scheduled delivery to independent partitions.
        for (var i = 0; i < _partitions.Length; i++)
        {
            var partition = i;
            tasks.Add(Task.Run(() => ScheduleAsync(partition, stop), CancellationToken.None));
        }
        if (Capabilities.ConsumptionModel == ConsumptionModel.Log && _groups.Count != 0)
        {
            var groups = _groups.Values.ToArray(); // Frozen once at first start; no per-message discovery.
            for (var i = 0; i < _partitions.Length; i++)
            {
                var partition = i;
                tasks.Add(Task.Run(() => FanOutAsync(partition, groups, stop), CancellationToken.None));
            }
        }
        foreach (var receiver in _receivers) tasks.Add(Task.Run(() => receiver.RunAsync(stop, abort), CancellationToken.None));
    }

    private async Task ScheduleAsync(int partition, CancellationToken stop)
    {
        var scheduled = _scheduled[partition];
        var due = _due[partition];
        var wake = _scheduleWake[partition];
        var timer = Clock.CreateTimer(static state => ((ScheduleWake)state!).Wake(), wake,
            Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        await using var timerLifetime = timer.ConfigureAwait(false);
        using var registration = stop.UnsafeRegister(static state => ((ScheduleWake)state!).Wake(), wake);
        var timerArmed = false;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                // Admission credits bound all retained messages, including the heap and group queues.
                while (scheduled.Reader.TryRead(out var incoming))
                    if (incoming.IsWakeup) wake.Consumed();
                    else
                        due.Enqueue(incoming, (incoming.DeliverAt!.Value, _nextScheduledSequence[partition]++));
                // Stop's wake-up may have been consumed by the drain above.
                if (stop.IsCancellationRequested) break;
                if (due.TryPeek(out var message, out var at))
                {
                    var delay = at.Due - Clock.GetUtcNow();
                    if (delay <= TimeSpan.Zero)
                    {
                        await _partitions[partition].Writer.WriteAsync(message, stop).ConfigureAwait(false);
                        due.Dequeue();
                        continue;
                    }
                    timer.Change(delay > TimeSpan.FromDays(1) ? TimeSpan.FromDays(1) : delay, Timeout.InfiniteTimeSpan);
                    timerArmed = true;
                }
                else if (timerArmed)
                {
                    timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                    timerArmed = false;
                }
                // Non-cancellable channel waits reuse the channel's operation. Timer and stop share
                // a sentinel wake-up, so neither needs a per-message cancellation registration.
                await scheduled.Reader.WaitToReadAsync().ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    private async Task FanOutAsync(int partition, Channel<InMemoryMessage>[][] groups, CancellationToken stop)
    {
        var state = _fanOut[partition];
        using var registration = state.Reader.RegisterStop(stop);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                if (state.Pending is null)
                {
                    state.Pending = await state.Reader.ReadAsync(stop).ConfigureAwait(false);
                    if (state.Reader.IsWakeup(state.Pending, stop))
                    {
                        state.Pending = null;
                        continue;
                    }
                    state.Pending.Position = state.NextPosition++;
                    state.Pending.References = groups.Length;
                    state.NextGroup = 0;
                }
                if (stop.IsCancellationRequested) break;
                while (state.NextGroup < groups.Length)
                {
                    await groups[state.NextGroup][partition].Writer.WriteAsync(state.Pending, stop).ConfigureAwait(false);
                    state.NextGroup++;
                }
                state.Pending = null;
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
    }

    internal void ValidateSettlement(Disposition disposition)
    {
        if (disposition.Kind is not (DispositionKind.Complete or DispositionKind.Redeliver or DispositionKind.DeadLetter))
            throw new InvalidOperationException($"Endpoint '{_name}': settlement requires an explicit Complete, Redeliver or DeadLetter decision.");
        if (disposition.Kind == DispositionKind.Redeliver &&
            (Capabilities.MaxRedeliveryDelay is not { } maximum || disposition.Delay > maximum))
            throw new InvalidOperationException($"Endpoint '{_name}': Redeliver exceeds MaxRedeliveryDelay or native redelivery is unavailable.");
        if (disposition.Kind == DispositionKind.DeadLetter && !Capabilities.DeadLetter)
            throw new InvalidOperationException($"Endpoint '{_name}': native DeadLetter is unavailable. Configure a core failure executor with a durable fallback.");
    }

    internal void Dispose()
    {
        foreach (var scheduled in _scheduled) { scheduled.Writer.TryComplete(); while (scheduled.Reader.TryRead(out _)) { } }
        foreach (var due in _due) due.Clear();
        foreach (var partition in _partitions) { partition.Writer.TryComplete(); while (partition.Reader.TryRead(out _)) { } }
        foreach (var group in _groups.Values)
            foreach (var partition in group) { partition.Writer.TryComplete(); while (partition.Reader.TryRead(out _)) { } }
        foreach (var receiver in _receivers) receiver.Clear();
        foreach (var state in _fanOut) state.Pending = null;
        Array.Clear(_pool);
        DeadLetters.Writer.TryComplete();
        _sendSlots.Writer.TryComplete();
    }

    private sealed class FanOut(InMemoryReadState reader)
    {
        internal InMemoryReadState Reader { get; } = reader;
        internal InMemoryMessage? Pending;
        internal int NextGroup;
        internal long NextPosition;
    }

    private sealed class ScheduleWake(ChannelWriter<InMemoryMessage> writer, InMemoryMessage wakeup)
    {
        private int _written;
        internal void Consumed() => Interlocked.Exchange(ref _written, 0);
        // Sentinels never own an admission credit. Preserve coalescing across stop/restart.
        internal void Wake()
        {
            if (Interlocked.Exchange(ref _written, 1) == 0) writer.TryWrite(wakeup);
        }
    }
}

internal sealed class InMemoryReceiver(InMemoryEndpoint endpoint, InMemoryReadState reader,
    InMemoryReceiveDelegate receive, string? group, int partition)
{
    private InMemoryMessage? _pending;
    private bool _manual;
    private Disposition _settlement;
    private ReadOnlyMemory<KeyValuePair<string, WireString>> _redeliveryHeaders;
    internal ReadOnlyMemory<KeyValuePair<string, WireString>> RedeliveryHeaders =>
        _redeliveryHeaders.IsEmpty ? _pending!.RedeliveryHeaders : _redeliveryHeaders;
    private Exception? _loggedSettlementError;
    private long _committed = -1;
    private TimeSpan _retryDelay;
    private long _retryStarted;
    internal string? Group { get; } = group;
    internal int Partition { get; } = partition;
    internal string PartitionId { get; } = partition.ToString(System.Globalization.CultureInfo.InvariantCulture);
    internal int Attempt { get; private set; } = 1;
    internal long CommittedPosition => Interlocked.Read(ref _committed);

    internal void Settle(Disposition disposition)
    {
        using var activity = endpoint.SettlementTracing?.Start(disposition, PartitionId, _pending);
        try
        {
            if (_pending is null || _manual) throw new InvalidOperationException("This native delivery is not active or has already been settled.");
            endpoint.ValidateSettlement(disposition);
            _settlement = disposition;
            _manual = true;
        }
        catch (Exception error)
        {
            if (endpoint.SettlementTracing?.Fail(activity, error, _pending) == true) _loggedSettlementError = error;
            throw;
        }
    }

    internal bool TryConsumeLoggedSettlementError(Exception error)
    {
        if (!ReferenceEquals(_loggedSettlementError, error)) return false;
        _loggedSettlementError = null;
        return true;
    }

    internal async Task RunAsync(CancellationToken stop, CancellationToken abort)
    {
        using var registration = reader.RegisterStop(stop);
        try
        {
            while (!stop.IsCancellationRequested)
            {
                if (_pending is null)
                {
                    _pending = await reader.ReadAsync(stop).ConfigureAwait(false);
                    if (reader.IsWakeup(_pending, stop))
                    {
                        _pending = null;
                        continue;
                    }
                    Attempt = _pending.QueueAttempt;
                }
                if (stop.IsCancellationRequested) break;
                while (_retryDelay > TimeSpan.Zero)
                {
                    var remaining = _retryDelay - endpoint.Clock.GetElapsedTime(_retryStarted);
                    if (remaining <= TimeSpan.Zero)
                    {
                        _retryDelay = TimeSpan.Zero;
                        break;
                    }
                    var delay = remaining > TimeSpan.FromDays(1) ? TimeSpan.FromDays(1) : remaining;
                    // Task.Delay truncates to whole milliseconds. Re-check elapsed time after each wake,
                    // and round up so a fractional remainder cannot become a synchronous busy loop.
                    delay = TimeSpan.FromMilliseconds(Math.Ceiling(delay.TotalMilliseconds));
                    await Task.Delay(delay, endpoint.Clock, stop).ConfigureAwait(false);
                }
                _manual = false;
                var delivery = new InMemoryDelivery(this, _pending);
                Disposition disposition;
                try { disposition = await receive(delivery, abort).ConfigureAwait(false); }
                catch { Attempt++; throw; }
                finally { _loggedSettlementError = null; }
                if (disposition.Kind == DispositionKind.SettledByHandler)
                {
                    if (!_manual) throw new InvalidOperationException("SettledByHandler requires successful native settlement through InMemoryDelivery.");
                    disposition = _settlement;
                }
                else if (_manual) throw new InvalidOperationException("Return SettledByHandler after manually settling InMemoryDelivery.");
                endpoint.ValidateSettlement(disposition);
                if (disposition.Kind == DispositionKind.Redeliver)
                {
                    if (!disposition.Headers.IsEmpty) _redeliveryHeaders = disposition.Headers;
                    Attempt++;
                    if (endpoint.Capabilities.ConsumptionModel == ConsumptionModel.Queue && !endpoint.Capabilities.OrderedByKey)
                    {
                        _pending.RedeliveryHeaders = RedeliveryHeaders;
                        _redeliveryHeaders = default;
                        _pending.QueueAttempt = Attempt;
                        _pending.DeliverAt = endpoint.Clock.GetUtcNow().Add(disposition.Delay);
                        await endpoint.EnqueueAsync(_pending, abort).ConfigureAwait(false);
                        _pending = null;
                    }
                    else
                    {
                        // Keep the ordered partition head until its monotonic redelivery deadline.
                        _retryDelay = disposition.Delay;
                        if (_retryDelay > TimeSpan.Zero) _retryStarted = endpoint.Clock.GetTimestamp();
                    }
                    continue;
                }
                if (disposition.Kind == DispositionKind.DeadLetter)
                {
                    endpoint.DeadLetters.Writer.TryWrite(new(delivery, disposition.DeadLetter));
                    endpoint.Observer?.DeadLettered(delivery, disposition.DeadLetter);
                }
                Interlocked.Exchange(ref _committed, _pending.Position);
                endpoint.Release(_pending);
                _pending = null;
                _redeliveryHeaders = default;
                Attempt = 1;
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested || abort.IsCancellationRequested) { }
    }

    internal void Clear() { _redeliveryHeaders = default; _pending = null; _loggedSettlementError = null; }
}

// One cancellation registration per loop, rather than one allocating channel read per message.
// A shared queue relays one wake-up to all readers. Admission leaves one channel slot for this wake-up.
internal sealed class InMemoryReadState(Channel<InMemoryMessage> channel, InMemoryMessage wakeup, bool shared = false)
{
    private int _wakeWritten;

    internal void Prepare()
    {
        // All previous loops have stopped. Remove the relayed wake-up before the next run.
        // Admission counts the data being rotated, so concurrent sends cannot take its channel slot.
        var count = channel.Reader.Count;
        for (var i = 0; i < count && channel.Reader.TryRead(out var message); i++)
            if (!message.IsWakeup && !channel.Writer.TryWrite(message))
                throw new InvalidOperationException("In-memory queue admission capacity was exceeded.");
        _wakeWritten = 0;
    }

    internal CancellationTokenRegistration RegisterStop(CancellationToken stop)
    {
        if (!shared) _wakeWritten = 0;
        return stop.UnsafeRegister(static state => ((InMemoryReadState)state!).Wake(), this);
    }

    private void Wake()
    {
        // Never depend on an individual reader's state: another reader may consume its wake-up.
        if (Interlocked.Exchange(ref _wakeWritten, 1) == 0) channel.Writer.TryWrite(wakeup);
    }

    internal ValueTask<InMemoryMessage> ReadAsync(CancellationToken stop)
    {
        stop.ThrowIfCancellationRequested();
        // Return the channel's source directly. A pooled async wrapper can still allocate when workers migrate.
        return channel.Reader.ReadAsync();
    }

    internal bool IsWakeup(InMemoryMessage message, CancellationToken stop)
    {
        if (!message.IsWakeup) return false;
        if (shared && stop.IsCancellationRequested) channel.Writer.TryWrite(wakeup);
        return true;
    }
}
