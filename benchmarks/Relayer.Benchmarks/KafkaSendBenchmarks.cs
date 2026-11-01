using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Producer;
using Dekaf.Serialization;
using Relayer.Kafka;

namespace Relayer.Benchmarks;

/// <summary>Broker-backed, acknowledged serialized sends. Set RELAYER_KAFKA_BOOTSTRAP_SERVERS to a test broker.</summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class KafkaSendBenchmarks
{
    private IKafkaProducer<byte[], byte[]> _raw = null!;
    private KafkaProducerTransport _transport = null!;
    private IAdminClient _admin = null!;
    private ProducerMessage<byte[], byte[]> _native = null!;
    private List<ProducerMessage<byte[], byte[]>> _nativeBatch = null!;
    private OutgoingMessage _message;
    private OutgoingMessage[] _batch = null!;
    private string _topic = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var bootstrap = Environment.GetEnvironmentVariable("RELAYER_KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new InvalidOperationException("Set RELAYER_KAFKA_BOOTSTRAP_SERVERS to a test Kafka broker.");
        _topic = "relayer-benchmark-" + Guid.NewGuid().ToString("N");
        _admin = new AdminClientBuilder().WithBootstrapServers(bootstrap).Build();
        await _admin.CreateTopicsAsync([new() { Name = _topic, NumPartitions = 3, ReplicationFactor = 1 }]);
        _raw = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).BuildAsync();
        var producer = await new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)>()
            .WithBootstrapServers(bootstrap).WithKeySerializer(new KafkaKeySerializer())
            .WithValueSerializer(new KafkaValueSerializer()).BuildAsync();
        _transport = new("benchmarks", producer, [_topic]);
        var body = Encoding.UTF8.GetBytes("{\"orderId\":42,\"customer\":\"customer-1\"}");
        var key = "customer-1"u8.ToArray();
        KeyValuePair<string, WireString>[] headers = [new("ce_type", new("benchmarks.order;v=1")),
            new("ce_source", new("/benchmarks")), new("ce_specversion", new("1.0")),
            new("ce_id", new("019a3c1b-97c0-7000-8000-000000000001")), new("content-type", new("application/json"))];
        var nativeHeaders = new Headers(headers.Select(header => new Header(header.Key, Encoding.UTF8.GetBytes(header.Value.Text!))));
        _native = new() { Topic = _topic, Key = key, Value = body, Headers = nativeHeaders };
        _message = new(_topic, body, headers, new WireString("customer-1"));
        _nativeBatch = Enumerable.Repeat(_native, 32).ToList();
        _batch = Enumerable.Repeat(_message, 32).ToArray();
        await _raw.ProduceAsync(_native);
        await _transport.SendAsync(_message, default);
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Single")]
    public ValueTask<RecordMetadata> RawSingle() => _raw.ProduceAsync(_native);

    [Benchmark, BenchmarkCategory("Single")]
    public ValueTask RelayerSingle() => _transport.SendAsync(_message, default);

    [Benchmark(Baseline = true, OperationsPerInvoke = 32), BenchmarkCategory("Batch")]
    public Task<RecordMetadata[]> RawBatch() => _raw.ProduceAllAsync(_nativeBatch);

    [Benchmark(OperationsPerInvoke = 32), BenchmarkCategory("Batch")]
    public ValueTask RelayerBatch() => _transport.SendBatchAsync(_batch, default);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_transport is not null) await _transport.DisposeAsync();
        if (_raw is not null) await _raw.DisposeAsync();
        if (_admin is not null)
        {
            try { if (_topic is not null) await _admin.DeleteTopicsAsync([_topic]); }
            finally { await _admin.DisposeAsync(); }
        }
    }
}
