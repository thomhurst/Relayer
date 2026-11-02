using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dekaf.SchemaRegistry;
using Dekaf.SchemaRegistry.Avro.Poco;
using Dekaf.Serialization;
using Relayer.Serialization.Avro;
using Relayer.TestFixtures;

namespace Relayer.Benchmarks;

/// <summary>Compares the adapter with the same prepared native Avro codecs, including segmented input copying.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class AvroSerializerBenchmarks
{
    private readonly AvroOrder _message = new() { Number = 42, Customer = "An order for Zoë" };
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly SerializationContext _context = new() { Topic = "orders", Component = SerializationComponent.Value };
    private SchemaRegistryClient _client = null!;
    private AvroMessageSerializer _serializer = null!;
    private AvroPocoSchemaRegistrySerializer<AvroOrder, AvroOrder.AvroCodec> _writer = null!;
    private AvroPocoSchemaRegistryDeserializer<AvroOrder, AvroOrder.AvroCodec> _reader = null!;
    private ReadOnlyMemory<byte> _body;
    private ReadOnlySequence<byte> _segmented;

    [GlobalSetup]
    public async Task Setup()
    {
        _client = new(new() { Url = "http://registry" }, new AvroRegistryResponses());
        _serializer = new AvroMessageSerializer(_client).Add<AvroOrder, AvroOrder.AvroCodec>("orders");
        await _serializer.InitializeAsync(default);
        _writer = new(_client);
        await _writer.WarmupAsync("orders");
        _reader = new(_client, new() { SchemaIdStrategy = SchemaIdDeserializerStrategy.Prefix });
        await _reader.WarmupAsync(7);
        _serializer.Serialize(_message, _buffer);
        _body = _buffer.WrittenMemory.ToArray();
        var first = new Segment(_body[..2]);
        var last = first.Append(_body[2..]);
        _segmented = new(first, 0, last, last.Memory.Length);
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Write")]
    public int NativeWrite()
    {
        _buffer.Clear();
        var buffer = _buffer;
        _writer.Serialize(_message, ref buffer, _context);
        return buffer.WrittenCount;
    }

    [Benchmark, BenchmarkCategory("Write")]
    public int RelayerWrite()
    {
        _buffer.Clear();
        _serializer.Serialize(_message, _buffer);
        return _buffer.WrittenCount;
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Read")]
    public AvroOrder NativeRead() => _reader.Deserialize(_body, _context);

    [Benchmark, BenchmarkCategory("Read")]
    public AvroOrder RelayerRead() => _serializer.Deserialize<AvroOrder>(new(_body));

    [Benchmark, BenchmarkCategory("Read")]
    public AvroOrder RelayerSegmentedRead() => _serializer.Deserialize<AvroOrder>(_segmented);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _serializer.DisposeAsync();
        await _writer.DisposeAsync();
        await _reader.DisposeAsync();
        _client.Dispose();
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        internal Segment(ReadOnlyMemory<byte> memory) => Memory = memory;
        internal Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
