using Relayer.Transports;
using BenchmarkDotNet.Attributes;
using Relayer.Outbox.SqlServer;

namespace Relayer.Benchmarks;

/// <summary>Attributes commit-to-worker buffer transfer and grouping separately from native database allocations.</summary>
[MemoryDiagnoser]
public class SqlServerOutboxDispatchBenchmarks
{
    private readonly List<SqlServerBufferedMessage> _source = [];
    private readonly OutboxDispatchBatch _batch = new();
    private readonly OutboxDispatchBatch _notification = new();
    private readonly List<SqlServerBufferedMessage> _one = [new()];
    private OutboxRecord[] _records = null!;
    private DateTimeOffset _now;

    [Params(1, 64)]
    public int Rows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _now = DateTimeOffset.UtcNow;
        _records = new OutboxRecord[Rows];
        var body = new byte[1024];
        KeyValuePair<string, WireString>[] headers = [new("ce_type", new WireString("dispatch.probe;v=1"))];
        for (var i = 0; i < Rows; i++)
        {
            _source.Add(new());
            _records[i] = new(Guid.CreateVersion7(), "events", "dispatch.probe;v=1",
                new OutgoingMessage(i % 2 == 0 ? "alpha" : "beta", body, headers), _now);
        }
        // Both halves of the buffer swap and header decode must reach their high-water marks.
        RecoveryBufferAndGroup();
        RecoveryBufferAndGroup();
        BufferOnly();
        TransferAndGroup();
        TransferAndGroup();
        CoalescedCommitProjection();
        CoalescedCommitProjection();
        CoalescedCommitProjection();
        SeparateCommitProjection();
    }

    // Same one-row commits without coalescing, before database/native I/O.
    [Benchmark]
    public int SeparateCommitProjection()
    {
        var bytes = 0;
        for (var i = 0; i < Rows; i++)
        {
            _one[0].Copy(_records[i], 32);
            _notification.Take(_one, 1, _now);
            _notification.Sort();
            bytes += _notification.Rows[0].ToMessage().Body.Length;
            _notification.Clear();
        }
        return bytes;
    }

    // Models several one-row committed notifications sharing one bounded working batch.
    // Includes ownership swaps, destination grouping and projection; excludes native/database I/O.
    [Benchmark]
    public int CoalescedCommitProjection()
    {
        for (var i = 0; i < Rows; i++)
        {
            _one[0].Copy(_records[i], 32);
            if (i == 0) _batch.Take(_one, 1, _now);
            else
            {
                _notification.Take(_one, 1, _now);
                _batch.Append(_notification);
            }
            _notification.Clear();
        }
        _batch.Sort();
        var bytes = 0;
        for (var i = 0; i < _batch.Count; i++) bytes += _batch.Rows[i].ToMessage().Body.Length;
        _batch.Clear();
        return bytes;
    }

    [Benchmark(Baseline = true)]
    public int BufferOnly()
    {
        var bytes = 0;
        for (var i = 0; i < Rows; i++)
        {
            _source[i].Copy(_records[i], 32);
            bytes += _source[i].ToMessage().Body.Length;
            _source[i].Clear();
        }
        return bytes;
    }

    [Benchmark]
    public int RecoveryBufferAndGroup()
    {
        for (var i = 0; i < Rows; i++) _source[i].Copy(_records[i], 32, unordered: true);
        _batch.Take(_source, Rows, _now);
        _batch.Sort();
        var bytes = 0;
        for (var i = 0; i < _batch.Count; i++) bytes += _batch.Rows[i].ToMessage().Body.Length;
        _batch.Clear();
        return bytes;
    }

    [Benchmark]
    public int TransferAndGroup()
    {
        for (var i = 0; i < Rows; i++) _source[i].Copy(_records[i], 32);
        _batch.Take(_source, Rows, _now);
        _batch.Sort();
        var bytes = 0;
        for (var i = 0; i < _batch.Count; i++) bytes += _batch.Rows[i].ToMessage().Body.Length;
        _batch.Clear();
        return bytes;
    }
}
