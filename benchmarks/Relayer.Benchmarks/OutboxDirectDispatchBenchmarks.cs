using BenchmarkDotNet.Attributes;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Compares buffered enlistment with commit handoff, grouping and serialized send projection; excludes database/native I/O.</summary>
[MemoryDiagnoser]
public class OutboxDirectDispatchBenchmarks
{
    private readonly List<BufferedOutboxMessage> _source = [];
    private readonly OutboxDispatchBatch _batch = new();
    private OutboxRecord _record;
    private DateTimeOffset _now;
    private OutgoingMessage[] _messages = [];
    private OutboxClaim[] _claims = [];
    private bool[] _claimed = [];

    [Params(1, 100)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _now = DateTimeOffset.UtcNow;
        _record = new(Guid.CreateVersion7(), "events", "benchmark.order;v=1",
            new OutgoingMessage("orders", new byte[256], new KeyValuePair<string, WireString>[]
            {
                new("ce_id", new WireString("00000000-0000-7000-8000-000000000001")),
                new("ce_type", new WireString("benchmark.order;v=1")),
                new("traceparent", new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))
            }), _now);
        for (var i = 0; i < MessageCount; i++) _source.Add(new());
        _messages = new OutgoingMessage[MessageCount];
        _claims = new OutboxClaim[MessageCount];
        _claimed = new bool[MessageCount];
        for (var i = 0; i < MessageCount; i++) _claimed[i] = i % 2 == 0;
        // Warm both sides of the ownership swap, including decoded header names.
        if (CommitHandoffAndProjection() != MessageCount * 256 || CommitHandoffAndProjection() != MessageCount * 256)
            throw new InvalidOperationException("The benchmark must project every unordered message.");
        if (CommitHandoffAndPartialClaimProjection() != ((MessageCount + 1) / 2) * 256)
            throw new InvalidOperationException("The benchmark must project only acquired claims.");
    }

    [Benchmark(Baseline = true)]
    public int EnlistOnly()
    {
        var bytes = 0;
        foreach (var row in _source)
        {
            row.Copy(_record, 32);
            bytes += row.Body.Length;
            row.Clear();
        }
        return bytes;
    }

    [Benchmark]
    public int CommitHandoffAndProjection()
    {
        foreach (var row in _source) row.Copy(_record, 32);
        _batch.Take(_source, _source.Count, _now);
        _batch.Sort();
        var bytes = 0;
        for (var i = 0; i < _batch.Count; i++) bytes += _batch.Rows[i].ToMessage().Body.Length;
        _batch.Clear();
        return bytes;
    }

    // Simulates alternating wins/losses from the database; includes token generation and reusable group cleanup.
    [Benchmark]
    public int CommitHandoffAndPartialClaimProjection()
    {
        foreach (var row in _source) row.Copy(_record, 32);
        _batch.Take(_source, _source.Count, _now);
        _batch.Sort();
        for (var i = 0; i < _batch.Count; i++)
        {
            _claims[i] = new(_batch.Rows[i].Id, Guid.NewGuid());
            _messages[i] = _batch.Rows[i].ToMessage();
        }
        var live = PostgreSqlDirectDispatcher.CompactClaims(_messages, _claims, _claimed);
        var bytes = 0;
        for (var i = 0; i < live; i++) bytes += _messages[i].Body.Length;
        Array.Clear(_messages);
        Array.Clear(_claims);
        _batch.Clear();
        return bytes;
    }
}
