using System.Buffers;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Warmed complete bursts below/above the retention cap, with normal/oversized bodies.</summary>
[MemoryDiagnoser]
public class SendRetentionBenchmarks
{
    private readonly Sink _sink = new();
    private SendRoute<byte[]> _route = null!;
    private ValueTask[] _pending = null!;
    private byte[] _body = null!;

    [Params(1, 64, 128)]
    public int Concurrency { get; set; }

    [Params(256, 131072)]
    public int BodySize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _body = new byte[BodySize];
        _pending = new ValueTask[Concurrency];
        _route = new(_sink, "orders", new("bench.retention", 1), new(), null, new Serializer(),
            static context => ((ISendOperation)context).DispatchAsync(), new());
        _sink.Initialize(Concurrency);
        DirectBurst();
    }

    [Benchmark(Baseline = true)]
    public void NativeBurst()
    {
        _sink.Reset();
        for (var i = 0; i < _pending.Length; i++) _pending[i] = _sink.SendAsync(new("orders", _body), default);
        Complete();
    }

    [Benchmark]
    public void DirectBurst()
    {
        _sink.Reset();
        for (var i = 0; i < _pending.Length; i++) _pending[i] = _route.SendAsync(_body, null, null, default, null);
        Complete();
    }

    private void Complete()
    {
        for (var i = 0; i < _pending.Length; i++)
        {
            _sink.Pending[i].Complete();
            _pending[i].GetAwaiter().GetResult();
        }
    }

    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/octet-stream";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => destination.Write((byte[])(object)message!);
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }

    private sealed class PendingSend : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _completion;
        internal void Reset() => _completion.Reset();
        internal ValueTask Wait() => new(this, _completion.Version);
        internal void Complete() => _completion.SetResult(true);
        public void GetResult(short token) => _completion.GetResult(token);
        public ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _completion.OnCompleted(continuation, state, token, flags);
    }

    private sealed class Sink : ITransport
    {
        internal PendingSend[] Pending = null!;
        private int _next;
        internal void Initialize(int count) => Pending = Enumerable.Range(0, count).Select(_ => new PendingSend()).ToArray();
        internal void Reset() { _next = 0; foreach (var pending in Pending) pending.Reset(); }
        public string Name => "sink";
        public TransportCapabilities GetCapabilities(string endpoint) => new();
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => Pending[_next++].Wait();
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => ValueTask.FromResult(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
