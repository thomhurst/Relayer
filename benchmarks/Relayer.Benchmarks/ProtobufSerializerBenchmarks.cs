using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Relayer.Serialization;
using Relayer.Serialization.Protobuf;

namespace Relayer.Benchmarks;

/// <summary>Compares adapter dispatch with the same native generated parser and reusable output buffer.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class ProtobufSerializerBenchmarks
{
    private readonly StringValue _message = new() { Value = "An order for Zoë" };
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly IMessageSerializer _serializer = new ProtobufMessageSerializer(types => types.Add(StringValue.Parser));
    private ReadOnlySequence<byte> _source;

    [GlobalSetup]
    public void Setup() => _source = new ReadOnlySequence<byte>(_message.ToByteArray());

    [Benchmark(Baseline = true), BenchmarkCategory("Write")]
    public int NativeWrite()
    {
        _buffer.Clear();
        _message.WriteTo(_buffer);
        return _buffer.WrittenCount;
    }

    [Benchmark, BenchmarkCategory("Write")]
    public int RelayerWrite()
    {
        _buffer.Clear();
        _serializer.Serialize(_message, _buffer);
        return _buffer.WrittenCount;
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Read")]
    public StringValue NativeRead() => StringValue.Parser.ParseFrom(_source);

    [Benchmark, BenchmarkCategory("Read")]
    public StringValue RelayerRead() => _serializer.Deserialize<StringValue>(_source);
}
