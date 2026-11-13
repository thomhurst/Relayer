using Relayer.Transports;
using BenchmarkDotNet.Attributes;
using Relayer.Outbox.MySql;

namespace Relayer.Benchmarks;

/// <summary>Attributes commit-to-worker buffer transfer and grouping separately from native database allocations.</summary>
[MemoryDiagnoser]
public class MySqlOutboxDispatchBenchmarks
{
    private readonly List<BufferedOutboxMessage> _source = [];
    private readonly OutboxDispatchBatch _batch = new();
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
        BufferOnly();
        TransferAndGroup();
        TransferAndGroup();
        RecoveryTransferAndGroup();
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
    public int RecoveryTransferAndGroup()
    {
        for (var i = 0; i < Rows; i++) _source[i].Copy(_records[i], 32);
        _batch.Take(_source, Rows, _now, recovered: true);
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

/// <summary>Measures reusable native batch parameter preparation as the dispatch group grows.</summary>
[MemoryDiagnoser]
public class MySqlDispatchMutationBenchmarks
{
    private readonly MySqlDispatchMutations _mutations = new("UPDATE outbox SET claim_token = @token WHERE id = @id");
    private OutboxClaim[] _claims = null!;
    private DateTimeOffset _now;
    private DateTimeOffset _expires;

    [Params(1, 64)]
    public int Rows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _now = DateTimeOffset.UtcNow;
        _expires = _now.AddMinutes(1);
        _claims = new OutboxClaim[Rows];
        for (var i = 0; i < Rows; i++) _claims[i] = new(Guid.NewGuid(), Guid.NewGuid());
        PrepareClaims();
    }

    [Benchmark]
    public void PrepareClaims() => _mutations.Prepare(_claims, _now, _expires);

    [GlobalCleanup]
    public void Cleanup() => _mutations.Dispose();
}
