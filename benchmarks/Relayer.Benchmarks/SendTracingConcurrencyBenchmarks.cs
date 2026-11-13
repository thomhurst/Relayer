using Relayer.Transports;
using System.Diagnostics;
using System.Threading.Tasks.Sources;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;

namespace Relayer.Benchmarks;

/// <summary>
/// Measures complete warmed bursts with deterministic overlapping native sends. Totals are per burst and include
/// Activity costs; SendTracingPoolTests separately verify retained preparation and continuation storage.
/// </summary>
[MemoryDiagnoser]
public class SendTracingConcurrencyBenchmarks
{
    private MessageTracing _tracing = null!;
    private SendTracing _send = null!;
    private ActivityListener _listener = null!;
    private Activity _ambient = null!;
    private PendingSend[] _native = null!;
    private ValueTask[] _pending = null!;
    private int _next;
    private readonly OutgoingMessage[] _messages = [new("orders", new byte[] { 1 }), new("orders", new byte[] { 2 })];

    [Params(1, 64, 128)]
    public int Concurrency { get; set; }

    [Params(false, true)]
    public bool Batch { get; set; }

    [Params("Drop", "Record")]
    public string Mode { get; set; } = "Drop";

    [GlobalSetup]
    public void Setup()
    {
        _tracing = new(Options.Create(new TracingOptions()), []);
        _listener = new()
        {
            ShouldListenTo = source => ReferenceEquals(source, _tracing.Source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                Mode == "Drop" ? ActivitySamplingResult.None : ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(_listener);
        _ambient = new Activity("request").Start();
        _native = new PendingSend[Concurrency];
        _pending = new ValueTask[Concurrency];
        for (var i = 0; i < _native.Length; i++) _native[i] = new();
        _send = new(_tracing, new() { System = "test.native", Destination = "orders" }, SendNative, SendBatchNative);
        for (var i = 0; i < 4; i++) TracedBurst();
    }

    [Benchmark(Baseline = true)]
    public void NativeBurst()
    {
        Reset();
        for (var i = 0; i < _pending.Length; i++)
            _pending[i] = Batch ? SendBatchNative(_messages, default) : SendNative(_messages[0], default);
        Complete();
    }

    [Benchmark]
    public void TracedBurst()
    {
        Reset();
        for (var i = 0; i < _pending.Length; i++)
            _pending[i] = Batch ? _send.SendBatchAsync(_messages) : _send.SendAsync(_messages[0]);
        Complete();
    }

    private void Reset()
    {
        _next = 0;
        foreach (var send in _native) send.Reset();
    }

    private ValueTask SendNative(OutgoingMessage message, CancellationToken cancellationToken) => _native[_next++].Wait();
    private ValueTask SendBatchNative(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => _native[_next++].Wait();

    private void Complete()
    {
        // Inline native completion makes each ValueTask terminal before consumption; GetResult never blocks.
        for (var i = 0; i < _pending.Length; i++)
        {
            _native[i].Complete();
            _pending[i].GetAwaiter().GetResult();
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ambient.Dispose();
        _listener.Dispose();
        _tracing.Dispose();
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
}
