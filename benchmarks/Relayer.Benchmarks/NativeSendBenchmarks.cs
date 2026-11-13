using Relayer.Transports;
using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Compares a preconfigured native send callback with direct native callback invocation.</summary>
[MemoryDiagnoser]
public class NativeSendBenchmarks
{
    private readonly NativeOutgoing _native = new();
    private Action<NativeOutgoing> _configure = null!;
    private OutgoingMessage _outgoing;

    [GlobalSetup]
    public void Setup()
    {
        _configure = static native => native.Value = 42;
        var options = new SendOptions().ConfigureNative(_configure);
        _outgoing = new OutgoingMessage("orders", ReadOnlyMemory<byte>.Empty, options: options);
    }

    [Benchmark(Baseline = true)]
    public int NativeCallback()
    {
        _configure(_native);
        return _native.Value;
    }

    [Benchmark]
    public int OutgoingNativeCallback()
    {
        _outgoing.ApplyNative(_native);
        return _native.Value;
    }

    public sealed class NativeOutgoing
    {
        public int Value { get; set; }
    }
}
