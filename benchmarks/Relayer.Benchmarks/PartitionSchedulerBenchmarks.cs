using Relayer.Transports;
using System.Buffers;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Isolates scheduler bookkeeping from serialization, native I/O and handler cost.</summary>
/// <remarks>The raw delegate baseline has no ordering or watermark tracking; ratios are not broker throughput overhead.</remarks>
[MemoryDiagnoser]
public class PartitionSchedulerBenchmarks
{
    private readonly Source _source = new();
    private readonly Delivery _first = new(1);
    private readonly Delivery _second = new(2);
    private readonly Delivery _sameKey = new(1);
    private Func<Delivery, CancellationToken, ValueTask<Disposition>> _receive = null!;
    private PartitionScheduler<Delivery, Binding, int, long> _scheduler = null!;
    private PartitionAssignmentToken _assignment;
    private long _position = 91;

    [Params(1, 16)]
    public int PartitionCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _receive = static (message, _) => message.Start();
        _scheduler = new(_source, _receive, new() { ConcurrencyPerPartition = 2 });
        _assignment = _scheduler.Assign(1);
        // Include retained, idle assignments in every pump's control and completion scans.
        for (var partition = 2; partition <= PartitionCount; partition++) _scheduler.Assign(partition);
    }

    [Benchmark(Baseline = true)]
    public DispositionKind RawSynchronousDispatch()
    {
        _first.Pending = false;
        var pending = _receive(_first, default);
        if (!pending.IsCompleted) throw new InvalidOperationException("Expected synchronous completion.");
        var result = pending.GetAwaiter().GetResult();
        _source.Commit(1, _position);
        return result.Kind;
    }

    [Benchmark]
    public long SynchronousDispatch()
    {
        _first.Pending = false;
        _scheduler.TryEnqueue(_assignment, _first, _position);
        _scheduler.Pump();
        return _source.Checkpoint;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public long UnrelatedKeysCompleteOutOfOrder()
    {
        _first.Pending = _second.Pending = true;
        _scheduler.TryEnqueue(_assignment, _first, _position);
        _scheduler.TryEnqueue(_assignment, _second, _position + 10);
        _scheduler.Pump();
        _second.Complete();
        _scheduler.Pump();
        _first.Complete();
        _scheduler.Pump();
        return _source.Checkpoint;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public long EqualKeysStayOrdered()
    {
        _first.Pending = _sameKey.Pending = true;
        _scheduler.TryEnqueue(_assignment, _first, _position);
        _scheduler.TryEnqueue(_assignment, _sameKey, _position + 10);
        _scheduler.Pump();
        _first.Complete();
        _scheduler.Pump();
        _sameKey.Complete();
        _scheduler.Pump();
        return _source.Checkpoint;
    }

    [GlobalCleanup]
    public void Cleanup() { _scheduler.Stop(TimeSpan.Zero); _scheduler.Pump(); }

    private sealed class Source : IPartitionSource<int, long>
    {
        internal long Checkpoint;
        public void Commit(int partition, long position) => Checkpoint = position;
        public void Pause(int partition) { }
        public void Resume(int partition) { }
    }

    private sealed class Delivery(int key) : IValueTaskSource<Disposition>
    {
        private ManualResetValueTaskSourceCore<Disposition> _completion;
        internal int Key { get; } = key;
        internal bool Pending;
        internal ValueTask<Disposition> Start()
        {
            if (!Pending) return new(Disposition.Complete);
            _completion.Reset();
            return new(this, _completion.Version);
        }
        internal void Complete() => _completion.SetResult(Disposition.Complete);
        public Disposition GetResult(short token) => _completion.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _completion.OnCompleted(continuation, state, token, flags);
    }

    private readonly struct Binding : IPartitionMessageBinding<Delivery>
    {
        public static bool KeysEqual(Delivery first, Delivery second) => first.Key == second.Key;
        public static ReadOnlySequence<byte> GetBody(Delivery message) => default;
        public static int? GetDeliveryCount(Delivery message) => null;
        public static bool TryGetHeader(Delivery message, string name, out WireString value) { value = default; return false; }
    }
}
