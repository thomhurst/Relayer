using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;

namespace Relayer.Benchmarks;

/// <summary>Isolates tracing primitive cost. End-to-end send/receive integration is tracked in #92 and #93.</summary>
[MemoryDiagnoser]
public class TracingBenchmarks
{
    private MessageTracing _tracing = null!;
    private MessageOperationTracing _send = null!;
    private MessageOperationTracing _process = null!;
    private MessageTraceData _delivery;
    private ActivityListener? _listener;

    [Params(false, true)]
    public bool Listener { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tracing = new(Options.Create(new TracingOptions()), []);
        var endpoint = new MessagingEndpoint { System = "relayer.inmemory", Destination = "orders" };
        _send = new(_tracing, endpoint, MessagingOperation.Send);
        _process = new(_tracing, endpoint, MessagingOperation.Process);
        _delivery = new()
        {
            CreationContext = new(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded),
            MessageId = "message-42", EventType = "orders;v=1",
        };
        if (Listener)
        {
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, _tracing.Source),
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            };
            ActivitySource.AddActivityListener(_listener);
        }
    }

    [Benchmark]
    public ActivityContext Send()
    {
        using var activity = _send.Start();
        return activity?.Context ?? default;
    }

    [Benchmark]
    public ActivityContext Process()
    {
        using var activity = _process.Start(_delivery);
        return activity?.Context ?? default;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _listener?.Dispose();
        _tracing.Dispose();
    }
}
