using System.Diagnostics;
using BenchmarkDotNet.Attributes;

namespace Relayer.Benchmarks;

/// <summary>Compares native lookup with the lazy header view over the same storage.</summary>
[MemoryDiagnoser]
public class HeaderReadBenchmarks
{
    private readonly Dictionary<string, WireString> _headers = new(StringComparer.Ordinal);
    private readonly HeaderNames _names = new();
    private string _key = null!;

    [GlobalSetup]
    public void Setup()
    {
        _key = _names.Type;
        _headers[_key] = new WireString("orders.placed;v=1"u8.ToArray());
    }

    [Benchmark(Baseline = true)]
    public WireString NativeLookup() => _headers[_key];

    [Benchmark]
    public WireString LazyLookup()
    {
        var headers = new MessageHeaders<Dictionary<string, WireString>, NativeReader>(_headers, _names);
        headers.TryGetValue(_key, out var value);
        return value;
    }

    [Benchmark]
    public MessageHeaders<Dictionary<string, WireString>, NativeReader> UnaccessedHeaders() => new(_headers, _names);

    public readonly struct NativeReader : IMessageHeaderReader<Dictionary<string, WireString>>
    {
        public static bool TryGetHeader(Dictionary<string, WireString> message, string name, out WireString value) =>
            message.TryGetValue(name, out value);
    }
}

/// <summary>Core formatting cost; reusable native storage excludes client allocations.</summary>
[MemoryDiagnoser]
public class EnvelopeWriteBenchmarks
{
    private readonly NativeBuffer _buffer = new();
    private MessageEnvelope _envelope = null!;
    private MessageEnvelopeMetadata _metadata;

    [GlobalSetup]
    public void Setup()
    {
        _envelope = new MessageEnvelope(new MessageType("orders.placed", 1), "/orders", new HeaderNames());
        _metadata = MessageEnvelopeMetadata.Create(new ActivityContext(
            ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded, "vendor=value"));
    }

    [Benchmark]
    public int WriteEnvelope()
    {
        _buffer.Length = 0;
        _envelope.Write<NativeBuffer, NativeWriter>(_buffer, _metadata);
        return _buffer.Length;
    }

    [Benchmark]
    public MessageEnvelopeMetadata CreateMetadata() => MessageEnvelopeMetadata.Create();

    public sealed class NativeBuffer
    {
        public char[] Buffer { get; } = new char[512];
        public int Length { get; set; }
    }

    public readonly struct NativeWriter : IMessageHeaderWriter<NativeBuffer>
    {
        public static void SetHeader(NativeBuffer message, string name, ReadOnlySpan<char> value)
        {
            value.CopyTo(message.Buffer.AsSpan(message.Length));
            message.Length += value.Length;
        }

        public static void RemoveHeader(NativeBuffer message, string name) { }
    }
}
