using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

[Message("bench.json-order")]
public sealed record JsonOrder(int Id, string Customer, decimal Total);

/// <summary>Compare Relayer with STJ using the same generated metadata and reusable buffers.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class JsonSerializerBenchmarks
{
    private readonly JsonOrder _message = new(42, "Ada", 12.5m);
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private ServiceProvider _provider = null!;
    private IMessageSerializer _serializer = null!;
    private Utf8JsonWriter _writer = null!;
    private ReadOnlySequence<byte> _source;
    private JsonTypeInfo<JsonOrder> _typeInfo = null!;

    [GlobalSetup]
    public void Setup()
    {
        _provider = new ServiceCollection().AddRelayer(builder => builder.AddMessageTypesFromThisAssembly()).BuildServiceProvider();
        _serializer = _provider.GetRequiredService<IMessageSerializer>();
        _typeInfo = (JsonTypeInfo<JsonOrder>)RelayerGeneratedJsonContext.Default.GetTypeInfo(typeof(JsonOrder))!;
        _serializer.Serialize(_message, _buffer);
        _source = new ReadOnlySequence<byte>(_buffer.WrittenMemory.ToArray());
        _buffer.Clear();
        _writer = new Utf8JsonWriter(_buffer);
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Write")]
    public int SystemTextJsonSerialize()
    {
        _buffer.Clear();
        _writer.Reset(_buffer);
        JsonSerializer.Serialize(_writer, _message, _typeInfo);
        _writer.Flush();
        return _buffer.WrittenCount;
    }

    [Benchmark]
    [BenchmarkCategory("Write")]
    public int RelayerSerialize()
    {
        _buffer.Clear();
        _serializer.Serialize(_message, _buffer);
        return _buffer.WrittenCount;
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Read")]
    public JsonOrder SystemTextJsonDeserialize()
    {
        var reader = new Utf8JsonReader(_source);
        return JsonSerializer.Deserialize(ref reader, _typeInfo)!;
    }

    [Benchmark]
    [BenchmarkCategory("Read")]
    public JsonOrder RelayerDeserialize() => _serializer.Deserialize<JsonOrder>(_source);

    [GlobalCleanup]
    public void Cleanup()
    {
        _writer.Dispose();
        _provider.Dispose();
    }
}
