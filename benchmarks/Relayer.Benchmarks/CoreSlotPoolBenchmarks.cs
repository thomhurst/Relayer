using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Core bounded scans and batch slot ownership, excluding startup allocations.</summary>
[MemoryDiagnoser]
public class CoreSlotPoolBenchmarks
{
    private const int Capacity = 64;
    private readonly SlotPool<object> _pool = new(Capacity, Capacity);
    private readonly object?[] _previous = new object?[Capacity];
    private readonly object[] _items = new object[Capacity];
    private int _cursor;
    private int _previousCursor;

    [GlobalSetup]
    public void Setup()
    {
        for (var i = 0; i < Capacity; i++)
        {
            _pool.TryReturn(new object(), i);
            _previous[i] = new object();
        }
    }

    [Benchmark(Baseline = true)]
    public void PreviousBounded()
    {
        for (var item = 0; item < Capacity; item++)
            for (var i = 0; i < Capacity; i++)
                if (Interlocked.Exchange(ref _previous[i], null) is { } value) { _items[item] = value; break; }
        for (var item = 0; item < Capacity; item++)
            for (var i = 0; i < Capacity; i++)
                if (Interlocked.CompareExchange(ref _previous[i], _items[item], null) is null) break;
    }

    [Benchmark]
    public void SharedBounded()
    {
        for (var i = 0; i < Capacity; i++) _items[i] = _pool.TryRent()!;
        for (var i = 0; i < Capacity; i++) _pool.TryReturn(_items[i]);
    }

    [Benchmark]
    public void PreviousGuarded()
    {
        for (var item = 0; item < Capacity; item++)
            for (var i = 0; i < Capacity; i++)
                if (Volatile.Read(ref _previous[i]) is not null &&
                    Interlocked.Exchange(ref _previous[i], null) is { } value) { _items[item] = value; break; }
        for (var item = 0; item < Capacity; item++)
            for (var i = 0; i < Capacity; i++)
                if (Volatile.Read(ref _previous[i]) is null &&
                    Interlocked.CompareExchange(ref _previous[i], _items[item], null) is null) break;
    }

    [Benchmark]
    public void SharedGuarded()
    {
        for (var i = 0; i < Capacity; i++) _items[i] = _pool.TryRentGuarded()!;
        for (var i = 0; i < Capacity; i++) _pool.TryReturnGuarded(_items[i]);
    }

    [Benchmark]
    public void PreviousIndexed()
    {
        var start = Volatile.Read(ref _previousCursor);
        for (var item = 0; item < Capacity; item++)
        {
            for (var offset = 0; offset < Capacity; offset++)
            {
                var index = start + offset;
                if (index >= Capacity) index -= Capacity;
                if (Interlocked.Exchange(ref _previous[index], null) is not { } value) continue;
                Volatile.Write(ref _previousCursor, index + 1 == Capacity ? 0 : index + 1);
                _items[index] = value;
                break;
            }
            start = Volatile.Read(ref _previousCursor);
        }
        for (var i = 0; i < Capacity; i++) Interlocked.CompareExchange(ref _previous[i], _items[i], null);
    }

    [Benchmark]
    public void SharedIndexed()
    {
        for (var i = 0; i < Capacity; i++) _items[i] = _pool.TryRent(ref _cursor)!;
        for (var i = 0; i < Capacity; i++) _pool.TryReturn(_items[i], i);
    }
}
