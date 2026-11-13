using System.Buffers;
using BenchmarkDotNet.Attributes;
using Dekaf.Serialization;
using Relayer.Kafka;

namespace Relayer.Benchmarks;

/// <summary>Isolates record-header work from broker latency; raw control uses native Add directly.</summary>
[MemoryDiagnoser]
public class KafkaSendHeaderBenchmarks
{
    private readonly KafkaValueSerializer _serializer = new();
    private readonly PreviousSerializer _previous = new();
    private ArrayBufferWriter<byte> _output = new(256);
    private Headers _application = null!;
    private Headers _native = null!;
    private Header _nativeHeader;

    [Params(8, 32)]
    public int HeaderCount { get; set; }

    [Params(false, true)]
    public bool Populated { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _application = new(HeaderCount);
        _native = new(HeaderCount + 1);
        for (var i = 0; i < HeaderCount; i++) _application.Add("header-" + i, "value");
        _nativeHeader = new("native-only", "value"u8.ToArray());
    }

    private void Reset()
    {
        _output.Clear();
        _native.Clear();
        if (Populated) _native.Add(_nativeHeader);
    }

    [Benchmark(Baseline = true)]
    public void RawNative()
    {
        Reset();
        for (var i = 0; i < _application.Count; i++) _native.Add(_application[i]);
    }

    [Benchmark]
    public void Relayer()
    {
        Reset();
        _serializer.Serialize((ReadOnlyMemory<byte>.Empty, _application), ref _output, new() { Headers = _native });
    }

    [Benchmark]
    public void Previous()
    {
        Reset();
        _previous.Serialize((ReadOnlyMemory<byte>.Empty, _application), ref _output, new() { Headers = _native });
    }

    // Frozen pre-#752 serializer with the same call boundary as the current serializer.
    private sealed class PreviousSerializer : ISerializer<(ReadOnlyMemory<byte> Body, Headers Headers)?>, IRecordHeaderSerializer
    {
        public bool ProducesRecordHeaders => true;

        public void Serialize<TWriter>((ReadOnlyMemory<byte> Body, Headers Headers)? record, ref TWriter destination,
            SerializationContext context) where TWriter : IBufferWriter<byte>, allows ref struct
        {
            var value = record.GetValueOrDefault();
            var headers = context.Headers ?? throw new InvalidOperationException("Dekaf did not provide record header storage.");
            if (value.Headers.GetFirst("traceparent") is not null)
            {
                headers.Remove("traceparent");
                headers.Remove("tracestate");
            }
            for (var i = 0; i < value.Headers.Count; i++) headers.Remove(value.Headers[i].Key);
            for (var i = 0; i < value.Headers.Count; i++) headers.Add(value.Headers[i]);
            value.Body.Span.CopyTo(destination.GetSpan(value.Body.Length));
            destination.Advance(value.Body.Length);
        }
    }
}
