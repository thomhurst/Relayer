using System.Diagnostics;
using System.Text;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Options;

namespace Relayer.Benchmarks;

/// <summary>Isolates primitive cost; SendTracingBenchmarks and ConsumeTracingBenchmarks cover pipeline integration.</summary>
[MemoryDiagnoser]
public class TracingBenchmarks
{
    private MessageTracing _tracing = null!;
    private MessageOperationTracing _send = null!;
    private MessageOperationTracing _process = null!;
    private MessageTraceData _delivery;
    private MessageTraceData _batch;
    private ActivityListener? _listener;

    [Params("NoListener", "Drop", "Record")]
    public string Mode { get; set; } = "NoListener";

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
            MessageId = "message-42", EventType = "orders;v=1", PartitionId = "3",
        };
        var links = new[] { new ActivityLink(_delivery.CreationContext), new ActivityLink(_delivery.CreationContext), new ActivityLink(_delivery.CreationContext) };
        _batch = new() { BatchCount = 2, Links = links.AsMemory(1, 2) };
        if (Mode != "NoListener")
        {
            _listener = new()
            {
                ShouldListenTo = source => ReferenceEquals(source, _tracing.Source),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                    Mode == "Drop" ? ActivitySamplingResult.None : ActivitySamplingResult.AllDataAndRecorded,
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

    [Benchmark]
    public ActivityContext Batch()
    {
        using var activity = _process.Start(_batch);
        return activity?.Context ?? default;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _listener?.Dispose();
        _tracing.Dispose();
    }
}

/// <summary>Separates wire decoding from the framework's ActivityContext ID storage cost.</summary>
[MemoryDiagnoser]
public class CreationContextBenchmarks
{
    private readonly HeaderNames _names = new();
    private Dictionary<string, WireString> _headers = null!;

    [Params(false, true)]
    public bool Utf8 { get; set; }

    [Params(false, true)]
    public bool TraceState { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        const string parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        _headers = new() { [_names.TraceParent] = Utf8 ? new(Encoding.UTF8.GetBytes(parent)) : new(parent) };
        if (TraceState) _headers[_names.TraceState] = Utf8 ? new(Encoding.UTF8.GetBytes("vendor=value")) : new("vendor=value");
    }

    [Benchmark]
    public ActivityContext Read() => MessageCreationContext.Read<Dictionary<string, WireString>, Reader>(_headers, _names);

    private sealed class Reader : IMessageHeaderReader<Dictionary<string, WireString>>
    {
        public static bool TryGetHeader(Dictionary<string, WireString> message, string name, out WireString value) => message.TryGetValue(name, out value);
    }
}
