using BenchmarkDotNet.Attributes;
using Relayer.Outbox.MongoDB;

namespace Relayer.Benchmarks;

/// <summary>Persistence encoding cost by payload size, excluding native driver I/O and native BSON document wrappers.</summary>
[MemoryDiagnoser]
public class MongoDBOutboxCodecBenchmarks
{
    [Params(128, 4096)]
    public int BodyBytes { get; set; }

    [Params(false, true)]
    public bool Ordered { get; set; }

    private readonly MongoDBOutboxBuffer _buffer = new();
    private OutboxRecord _record;

    [GlobalSetup]
    public void Setup()
    {
        var body = new byte[BodyBytes];
        new Random(42).NextBytes(body);
        _record = new(Guid.CreateVersion7(), "events", "order;v=1", new("orders", body,
            new KeyValuePair<string, WireString>[] { new("type", new("order;v=1")), new("traceparent", new("original-creation-context")) },
            Ordered ? new WireString("customer-é") : null), DateTimeOffset.UtcNow);
        _buffer.Encode(_record, 32);
    }

    [Benchmark]
    public int Encode() => _buffer.Encode(_record, 32);
}
