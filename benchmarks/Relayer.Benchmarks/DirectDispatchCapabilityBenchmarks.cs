using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Relayer.Outbox;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Endpoint lookup with hot sets and cache churn; excludes native capability allocation and I/O.</summary>
[MemoryDiagnoser]
public class DirectDispatchCapabilityBenchmarks
{
    private readonly Dictionary<(string Transport, string Destination), TransportCapabilities> _unbounded = [];
    private readonly DirectDispatchCapabilityCache _bounded = new();
    private readonly Transport _transport = new();
    private string[] _destinations = [];
    private int _unboundedIndex;
    private int _boundedIndex;

    [Params(1, 256, 512)]
    public int DestinationCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _destinations = new string[DestinationCount];
        for (var i = 0; i < DestinationCount; i++)
        {
            var destination = "tenant-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            _destinations[i] = destination;
            _unbounded.Add((_transport.Name, destination), _transport.GetCapabilities(destination));
            _bounded.Get(_transport, destination);
        }
    }

    [Benchmark(Baseline = true)]
    public TransportCapabilities Unbounded()
    {
        var destination = _destinations[_unboundedIndex];
        if (++_unboundedIndex == DestinationCount) _unboundedIndex = 0;
        return PreviousLookup(destination);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private TransportCapabilities PreviousLookup(string destination)
    {
        var key = (_transport.Name, destination);
        if (!_unbounded.TryGetValue(key, out var capabilities))
        {
            capabilities = _transport.GetCapabilities(destination);
            if (capabilities.MaxBatchSize <= 0) throw new InvalidOperationException();
            _unbounded.Add(key, capabilities);
        }
        return capabilities;
    }

    [Benchmark]
    public TransportCapabilities Bounded()
    {
        var destination = _destinations[_boundedIndex];
        if (++_boundedIndex == DestinationCount) _boundedIndex = 0;
        return _bounded.Get(_transport, destination);
    }

    // Native clients may allocate their own capabilities. Reuse this fixture to isolate Relayer's cost.
    private sealed class Transport : ITransport
    {
        private readonly TransportCapabilities _capabilities = new() { MaxBatchSize = 100 };
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => _capabilities;
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => ValueTask.CompletedTask;
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
