using Relayer.Transports;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
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
    private DirectJsonSend _json = null!;
    private IKafkaProducer<byte[], ReadOnlyMemory<byte>> _rawJson = null!;
    private readonly byte[] _jsonKey = "customer-1"u8.ToArray();
    private readonly NativeHeaders _jsonHeaders = new();
    private byte[] _jsonContentType = null!;

    [Params(false, true)]
    public bool Tombstone { get; set; }

    [Params(false, true)]
    public bool NativeRecord { get; set; }

    private RelayerInstrumentation _instrumentation = null!;
    private MessageTracing _tracing = null!;
    private MeterListener? _metrics;

    [Params(false, true)]
    public bool Metrics { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _instrumentation = new();
        _tracing = new(Options.Create(new TracingOptions()), [], instrumentation: _instrumentation);
        if (Metrics)
        {
            _metrics = new()
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument.Meter.Scope, _instrumentation)) listener.EnableMeasurementEvents(instrument);
                },
            };
            _metrics.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
            _metrics.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
            _metrics.Start();
        }
        var bootstrap = Environment.GetEnvironmentVariable("RELAYER_KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new InvalidOperationException("Set RELAYER_KAFKA_BOOTSTRAP_SERVERS to a test Kafka broker.");
        _topic = "relayer-benchmark-" + Guid.NewGuid().ToString("N");
        _admin = new AdminClientBuilder().WithBootstrapServers(bootstrap).Build();
        await _admin.CreateTopicsAsync([new() { Name = _topic, NumPartitions = 3, ReplicationFactor = 1 }]);
        _raw = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).BuildAsync();
        _rawJson = await new ProducerBuilder<byte[], ReadOnlyMemory<byte>>().WithBootstrapServers(bootstrap)
            .WithValueSerializer(Serializers.RawBytes).BuildAsync();
        var producer = await new ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Headers Headers)?>()
            .WithBootstrapServers(bootstrap).WithKeySerializer(new KafkaKeySerializer())
            .WithValueSerializer(new KafkaValueSerializer()).BuildAsync();
        _transport = new("benchmarks", producer, [_topic], _tracing);
        var body = Encoding.UTF8.GetBytes("{\"orderId\":42,\"customer\":\"customer-1\"}");
        var key = "customer-1"u8.ToArray();
        KeyValuePair<string, WireString>[] headers = [new("ce_type", new("benchmarks.order;v=1")),
            new("ce_source", new("/benchmarks")), new("ce_specversion", new("1.0")),
            new("ce_id", new("019a3c1b-97c0-7000-8000-000000000001")), new("content-type", new("application/json"))];
        var nativeHeaders = new Headers(headers.Select(header => new Header(header.Key, Encoding.UTF8.GetBytes(header.Value.Text!))));
        _native = new() { Topic = _topic, Key = key, Value = Tombstone ? null! : body, Headers = nativeHeaders };
        var options = NativeRecord ? new SendOptions().KafkaRecord(static record => record with { Partition = 1,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(1700000000000) }) : null;
        if (NativeRecord) _native = _native with { Partition = 1, Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(1700000000000) };
        _message = new(_topic, Tombstone ? default : body, headers, new WireString("customer-1"), options: options, isTombstone: Tombstone);
        _nativeBatch = Enumerable.Repeat(_native, 32).ToList();
        _batch = Enumerable.Repeat(_message, 32).ToArray();
        _json = new(_instrumentation, builder => builder.Kafka("benchmark-json", kafka =>
        {
            kafka.TopologyMode = TopologyMode.None;
            kafka.Options.Configure(options => options.BootstrapServers.Add(bootstrap));
            kafka.Send<DirectJsonOrder>(_topic, route => route.OrderingKey(static order => order.Customer));
        }));
        await _json.StartAsync();
        _jsonContentType = Encoding.UTF8.GetBytes(_json.ContentType);
        await RawJsonSingle();
        await RelayerJsonSingle();
        await _raw.ProduceAsync(_native);
        await _transport.SendAsync(_message, default);
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Single")]
    public ValueTask<RecordMetadata> RawSingle() => _raw.ProduceAsync(_native);

    [Benchmark, BenchmarkCategory("Single")]
    public ValueTask RelayerSingle() => _transport.SendAsync(_message, default);

    [Benchmark(Baseline = true), BenchmarkCategory("DefaultJson")]
    public ValueTask<RecordMetadata> RawJsonSingle()
    {
        var body = Tombstone ? default : _json.Serialize();
        _jsonHeaders.Clear();
        _json.WriteEnvelope<NativeHeaders, NativeHeaders>(_jsonHeaders);
        if (!Tombstone) _jsonHeaders.Headers.Add("content-type", _jsonContentType);
        return Tombstone
            ? _raw.ProduceAsync(_topic, _jsonKey, null!, headers: _jsonHeaders.Headers)
            : _rawJson.ProduceAsync(_topic, _jsonKey, body, headers: _jsonHeaders.Headers);
    }

    [Benchmark, BenchmarkCategory("DefaultJson")]
    public ValueTask RelayerJsonSingle() => Tombstone
        ? _json.Sender.SendAsync(global::Relayer.Tombstone.For<DirectJsonOrder>("customer-1"))
        : _json.Sender.SendAsync(_json.Order);

    // The native headers borrow this reusable encoding buffer until the acknowledged send returns.
    private sealed class NativeHeaders : IMessageHeaderWriter<NativeHeaders>
    {
        internal readonly Headers Headers = new();
        private byte[] _utf8 = new byte[512];
        private int _position;

        internal void Clear() { Headers.Clear(); _position = 0; }

        public static void SetHeader(NativeHeaders state, string name, ReadOnlySpan<char> value)
        {
            var required = Encoding.UTF8.GetByteCount(value);
            if (state._utf8.Length - state._position < required)
                Array.Resize(ref state._utf8, Math.Max(state._utf8.Length * 2, state._position + required));
            var written = Encoding.UTF8.GetBytes(value, state._utf8.AsSpan(state._position));
            state.Headers.Add(new Header(name, state._utf8.AsMemory(state._position, written)));
            state._position += written;
        }
        // Clear removes every header before a fresh envelope is written.
        public static void RemoveHeader(NativeHeaders state, string name) { }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = 32), BenchmarkCategory("Batch")]
    public Task<RecordMetadata[]> RawBatch() => _raw.ProduceAllAsync(_nativeBatch);

    [Benchmark(OperationsPerInvoke = 32), BenchmarkCategory("Batch")]
    public ValueTask RelayerBatch() => _transport.SendBatchAsync(_batch, default);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_json is not null) await _json.DisposeAsync();
        if (_transport is not null) await _transport.DisposeAsync();
        if (_rawJson is not null) await _rawJson.DisposeAsync();
        if (_raw is not null) await _raw.DisposeAsync();
        if (_admin is not null)
        {
            try { if (_topic is not null) await _admin.DeleteTopicsAsync([_topic]); }
            finally { await _admin.DisposeAsync(); }
        }
        _metrics?.Dispose();
        _tracing?.Dispose();
        _instrumentation?.Dispose();
    }
}
