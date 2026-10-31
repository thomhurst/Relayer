using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Per-message lookups that must stay allocation-free (docs/decisions.md D18).</summary>
[MemoryDiagnoser]
public class HotPathBenchmarks
{
    private readonly MessageFeatures _features = new();
    private readonly MessageTypeRegistry _registry = new();

    [GlobalSetup]
    public void Setup()
    {
        _features.Set(new FeatureA());
        _features.Set(new FeatureB());
        _features.Set(new FeatureC());

        _registry.Add<FeatureA>("bench.a").Add<FeatureB>("bench.b").Add<FeatureC>("bench.c");
        _registry.Freeze();
    }

    [Benchmark]
    public FeatureC? FeatureLookup() => _features.Get<FeatureC>();

    [Benchmark]
    public bool MessageTypeLookup() => _registry.TryGetMessageType(typeof(FeatureC), out _);

    public sealed class FeatureA;
    public sealed class FeatureB;
    public sealed class FeatureC;
}
