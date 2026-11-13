using BenchmarkDotNet.Attributes;
using Dekaf;
using Dekaf.Admin;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Relayer.Kafka;

namespace Relayer.Benchmarks;

/// <summary>Isolates native header materialization from broker, JSON, DI and dispatch costs.</summary>
/// <remarks>The enumerator holds one real fetch batch throughout measurement. No benchmark advances it.</remarks>
[MemoryDiagnoser]
public class KafkaHeaderAllocationBenchmarks
{
    private IAdminClient _admin = null!;
    private IKafkaConsumer<byte[], byte[]> _consumer = null!;
    private IAsyncEnumerator<ConsumeBatch<byte[], byte[]>> _batches = null!;
    private ConsumeResult<byte[], byte[]> _record;
    private KafkaDelivery _delivery;
    private string _topic = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var bootstrap = Environment.GetEnvironmentVariable("RELAYER_KAFKA_BOOTSTRAP_SERVERS")
            ?? throw new InvalidOperationException("Set RELAYER_KAFKA_BOOTSTRAP_SERVERS to a test Kafka broker.");
        _topic = "relayer-header-benchmark-" + Guid.NewGuid().ToString("N");
        _admin = new AdminClientBuilder().WithBootstrapServers(bootstrap).Build();
        await _admin.CreateTopicsAsync([new() { Name = _topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        await using (var producer = await new ProducerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).BuildAsync())
        {
            await producer.ProduceAsync(new()
            {
                Topic = _topic, Value = "{\"Value\":42}"u8.ToArray(),
                Headers = new Headers().Add("ce_id", "benchmark").Add("ce_type", "benchmarks.kafka-receive;v=1"),
            });
        }
        _consumer = new ConsumerBuilder<byte[], byte[]>().WithBootstrapServers(bootstrap).WithGroupId(_topic)
            .WithOffsetCommitMode(OffsetCommitMode.Manual).WithAutoOffsetStore(false).WithAutoOffsetReset(AutoOffsetReset.Earliest).Build();
        _consumer.Assign(new TopicPartition(_topic, 0));
        await _consumer.InitializeAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        _batches = _consumer.ConsumeBatchAsync(timeout.Token).GetAsyncEnumerator();
        if (!await _batches.MoveNextAsync()) throw new InvalidOperationException("Missing native fetch batch.");
        var records = _batches.Current.GetEnumerator();
        if (!records.MoveNext()) throw new InvalidOperationException("Empty native fetch batch.");
        _record = records.Current;
        _delivery = new(_record, "0");
        if (NativeSingleLookup() != 1 || NativeEnvelopeLookups() != 2 || SharedNativeSnapshot() != 2 || RelayerEnvelopeLookups() != 2)
            throw new InvalidOperationException("Header controls disagree on the known envelope.");
    }

    [Benchmark(Baseline = true)]
    public long NativeNoHeaders() => _record.Offset;

    [Benchmark]
    public int NativeSingleLookup() => Find(_record.Headers, "ce_type") ? 1 : 0;

    // The executable success path validates type/id and reads both failure counters.
    // Each lookup deliberately obtains the native property again to model the former binding.
    [Benchmark]
    public int NativeEnvelopeLookups()
        => (Find(_record.Headers, "ce_type") ? 1 : 0)
            + (Find(_record.Headers, "ce_id") ? 1 : 0)
            + (Find(_record.Headers, FailureHeaders.Attempts) ? 1 : 0)
            + (Find(_record.Headers, FailureHeaders.Redeliveries) ? 1 : 0);

    [Benchmark]
    public int SharedNativeSnapshot()
    {
        var headers = _record.Headers;
        return (Find(headers, "ce_type") ? 1 : 0)
            + (Find(headers, "ce_id") ? 1 : 0)
            + (Find(headers, FailureHeaders.Attempts) ? 1 : 0)
            + (Find(headers, FailureHeaders.Redeliveries) ? 1 : 0);
    }

    [Benchmark]
    public int RelayerEnvelopeLookups()
        => (KafkaMessageBinding.TryGetHeader(_delivery, "ce_type", out _) ? 1 : 0)
            + (KafkaMessageBinding.TryGetHeader(_delivery, "ce_id", out _) ? 1 : 0)
            + (KafkaMessageBinding.TryGetHeader(_delivery, FailureHeaders.Attempts, out _) ? 1 : 0)
            + (KafkaMessageBinding.TryGetHeader(_delivery, FailureHeaders.Redeliveries, out _) ? 1 : 0);

    private static bool Find(IReadOnlyList<Header> headers, string name)
    {
        for (var i = 0; i < headers.Count; i++)
            if (headers[i].Key == name) return true;
        return false;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_batches is not null) await _batches.DisposeAsync();
        if (_consumer is not null) await _consumer.DisposeAsync();
        if (_admin is not null)
        {
            try { if (_topic is not null) await _admin.DeleteTopicsAsync([_topic]); }
            finally { await _admin.DisposeAsync(); }
        }
    }
}
