using System.Buffers;
using System.Net;
using System.Text;
using System.Text.Json.Serialization;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dekaf.SchemaRegistry;
using Dekaf.Serialization;
using Relayer.Kafka;

namespace Relayer.Benchmarks;

/// <summary>Compare prepared registry serialization with native Dekaf using identical generated metadata.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class KafkaSchemaSerializerBenchmarks
{
    private const string Schema = """{"type":"object","properties":{"Number":{"type":"integer"}}}""";
    private readonly RegistryOrder _message = new(42);
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private readonly SerializationContext _context = new() { Topic = "orders", Component = SerializationComponent.Value };
    private SchemaRegistryClient _client = null!;
    private JsonSchemaRegistrySerializer<RegistryOrder> _nativeWriter = null!;
    private JsonSchemaRegistryDeserializer<RegistryOrder> _nativeReader = null!;
    private KafkaJsonSchemaSerializer _serializer = null!;
    private ReadOnlyMemory<byte> _body;

    [GlobalSetup]
    public async Task Setup()
    {
        // HTTP is used only for startup preparation, never by a measured operation.
        _client = new(new() { Url = "http://registry" }, new RegistryResponses());
        _nativeWriter = new(_client, Schema, RegistryBenchmarkJson.Default.RegistryOrder);
        _nativeReader = new(_client, RegistryBenchmarkJson.Default.RegistryOrder);
        await _nativeWriter.PrepareAsync("orders", _message);
        _ = await _client.GetSchemaAsync(7);
        _serializer = new KafkaJsonSchemaSerializer(_client).Add("orders", Schema, RegistryBenchmarkJson.Default.RegistryOrder);
        await _serializer.InitializeAsync(default);
        _serializer.Serialize(_message, _buffer);
        _body = _buffer.WrittenMemory.ToArray();
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Write")]
    public int DekafWrite()
    {
        _buffer.Clear();
        var writer = _buffer;
        _nativeWriter.Serialize(_message, ref writer, _context);
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
    public RegistryOrder DekafRead() => _nativeReader.Deserialize(_body, _context);

    [Benchmark, BenchmarkCategory("Read")]
    public RegistryOrder RelayerRead() => _serializer.Deserialize<RegistryOrder>(new(_body));

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _serializer.DisposeAsync();
        await _nativeWriter.DisposeAsync();
        await _nativeReader.DisposeAsync();
        _client.Dispose();
    }

    private sealed class RegistryResponses : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            const string response = """{"id":7,"subject":"orders-value","version":1,"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"Number\":{\"type\":\"integer\"}}}"}""";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            { Content = new StringContent(response, Encoding.UTF8, "application/json") });
        }
    }
}

public sealed record RegistryOrder(int Number);
[JsonSerializable(typeof(RegistryOrder))]
internal partial class RegistryBenchmarkJson : JsonSerializerContext;
