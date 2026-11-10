using BenchmarkDotNet.Attributes;
using Relayer.GooglePubSub;

namespace Relayer.Benchmarks;

/// <summary>Measures bounded native-boundary state retention, including bursts beyond capacity.</summary>
[MemoryDiagnoser]
public class GooglePubSubPoolBenchmarks
{
    [Params(1, 130, 256, 512)]
    public int Concurrency { get; set; }

    private GooglePubSubPool<object> _pool = null!;
    private object[] _items = null!;

    [GlobalSetup]
    public void Setup()
    {
        _pool = new(static () => new());
        _items = new object[Concurrency];
        RentAndReturnBurst();
    }

    [Benchmark]
    public void RentAndReturnBurst()
    {
        for (var i = 0; i < _items.Length; i++) _items[i] = _pool.Rent();
        for (var i = 0; i < _items.Length; i++)
        {
            _pool.Return(_items[i]);
            _items[i] = null!;
        }
    }
}
