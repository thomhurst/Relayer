using System.Buffers;
using BenchmarkDotNet.Attributes;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Storage codec cost by header count, independently of database/native-client allocations.</summary>
[MemoryDiagnoser]
public class OutboxHeaderCodecBenchmarks
{
    [Params(4, 16)]
    public int HeaderCount { get; set; }

    private KeyValuePair<string, WireString>[] _headers = [];
    private KeyValuePair<string, WireString>[] _decoded = [];
    private readonly ArrayBufferWriter<byte> _buffer = new(4096);

    [GlobalSetup]
    public void Setup()
    {
        _headers = new KeyValuePair<string, WireString>[HeaderCount];
        for (var i = 0; i < _headers.Length; i++) _headers[i] = new("header-" + i, new WireString("native-value-é"));
        OutboxHeaderCodec.Write(_buffer, _headers);
        OutboxHeaderCodec.Read(_buffer.WrittenMemory, ref _decoded);
    }

    [Benchmark]
    public int Encode()
    {
        _buffer.Clear();
        OutboxHeaderCodec.Write(_buffer, _headers);
        return _buffer.WrittenCount;
    }

    [Benchmark]
    public int Decode() => OutboxHeaderCodec.Read(_buffer.WrittenMemory, ref _decoded).Length;
}
