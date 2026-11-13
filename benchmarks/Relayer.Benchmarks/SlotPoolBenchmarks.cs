using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Compares the previous transport slot scan with the shared implementation.</summary>
[MemoryDiagnoser]
public class SlotPoolBenchmarks
{
    [Params(1, 130, 512)]
    public int Concurrency { get; set; }

    [Params(256, int.MaxValue)]
    public int Capacity { get; set; }

    private SlotPool<object> _pool = null!;
    private PreviousPool _previous = null!;
    private object[] _items = null!;

    [GlobalSetup]
    public void Setup()
    {
        _pool = new(capacity: Capacity);
        _previous = new(Capacity);
        _items = new object[Concurrency];
        Previous();
        Shared();
    }

    [Benchmark(Baseline = true)]
    public void Previous()
    {
        for (var i = 0; i < _items.Length; i++) _items[i] = _previous.Rent();
        for (var i = 0; i < _items.Length; i++)
        {
            _previous.Return(_items[i]);
            _items[i] = null!;
        }
    }

    [Benchmark]
    public void Shared()
    {
        for (var i = 0; i < _items.Length; i++) _items[i] = _pool.TryRent() ?? new object();
        for (var i = 0; i < _items.Length; i++)
        {
            _pool.TryReturn(_items[i]);
            _items[i] = null!;
        }
    }

    // Previous Google Pub/Sub scan; the other five transports use its unbounded variant.
    private sealed class PreviousPool(int capacity)
    {
        private readonly Page _first = new();
        internal object Rent()
        {
            for (Page? page = _first; page is not null; page = Volatile.Read(ref page.Next))
                for (var i = 0; i < page.Slots.Length; i++)
                    if (Interlocked.Exchange(ref page.Slots[i], null) is { } item) return item;
            return new();
        }
        internal void Return(object item)
        {
            var page = _first;
            var pages = capacity == int.MaxValue ? int.MaxValue : capacity / 64;
            for (var pageIndex = 0; pageIndex < pages; pageIndex++)
            {
                for (var i = 0; i < page.Slots.Length; i++)
                    if (Interlocked.CompareExchange(ref page.Slots[i], item, null) is null) return;
                if (pageIndex == pages - 1) return;
                var next = Volatile.Read(ref page.Next);
                if (next is null) next = Interlocked.CompareExchange(ref page.Next, new Page(), null) ?? page.Next!;
                page = next;
            }
        }
        private sealed class Page
        {
            internal readonly object?[] Slots = new object?[64];
            internal Page? Next;
        }
    }
}
