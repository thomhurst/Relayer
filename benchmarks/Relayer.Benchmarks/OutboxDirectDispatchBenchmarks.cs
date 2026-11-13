using Relayer.Transports;
using System.Diagnostics.Metrics;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Compares buffered enlistment with commit handoff, grouping and serialized send projection; excludes database/native I/O.</summary>
[MemoryDiagnoser]
public class OutboxDirectDispatchBenchmarks
{
    private readonly List<BufferedOutboxMessage> _source = [];
    private readonly OutboxDispatchBatch _batch = new();
    private readonly OutboxDispatchBatch _notification = new();
    private readonly List<BufferedOutboxMessage> _one = [new()];
    private OutboxRecord _record;
    private DateTimeOffset _now;
    private OutgoingMessage[] _messages = [];
    private OutboxClaim[] _claims = [];
    private bool[] _claimed = [];
    private DateTimeOffset[] _created = [];
    private readonly RelayerInstrumentation _instrumentation = new();
    private MeterListener? _listener;

    private NpgsqlDataSource _dataSource = null!;
    private PostgreSqlDirectDispatcher _recovery = null!;
    private OutboxRecord[] _records = [];

    [Params(false, true)]
    public bool MetricsEnabled { get; set; }

    [Params(1, 100)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        if (MetricsEnabled)
        {
            _listener = new()
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument.Meter.Scope, _instrumentation)) listener.EnableMeasurementEvents(instrument);
                },
            };
            _listener.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
            _listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
            _listener.Start();
        }
        _now = DateTimeOffset.UtcNow;
        _record = new(Guid.CreateVersion7(), "events", "benchmark.order;v=1",
            new OutgoingMessage("orders", new byte[256], new KeyValuePair<string, WireString>[]
            {
                new("ce_id", new WireString("00000000-0000-7000-8000-000000000001")),
                new("ce_type", new WireString("benchmark.order;v=1")),
                new("traceparent", new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))
            }), _now);
        for (var i = 0; i < MessageCount; i++) _source.Add(new());
        _records = new OutboxRecord[MessageCount];
        Array.Fill(_records, _record);
        _dataSource = NpgsqlDataSource.Create("Host=unused");
        _recovery = new(_dataSource, new(), new() { BatchSize = MessageCount, QueueCapacity = 1 }, [], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        RecoveryProjection();
        RecoveryProjection();
        _messages = new OutgoingMessage[MessageCount];
        _claims = new OutboxClaim[MessageCount];
        _claimed = new bool[MessageCount];
        _created = new DateTimeOffset[MessageCount];
        for (var i = 0; i < MessageCount; i++) _claimed[i] = i % 2 == 0;
        // Warm both sides of the ownership swap, including decoded header names.
        if (CommitHandoffAndProjection() != MessageCount * 256 || CommitHandoffAndProjection() != MessageCount * 256)
            throw new InvalidOperationException("The benchmark must project every unordered message.");
        if (CommitHandoffAndPartialClaimProjection() != ((MessageCount + 1) / 2) * 256)
            throw new InvalidOperationException("The benchmark must project only acquired claims.");
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
        for (var i = 0; i < MessageCount; i++)
        {
            _one[0].Copy(_record, 32);
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
        for (var i = 0; i < MessageCount; i++)
        {
            _one[0].Copy(_record, 32);
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

    [GlobalCleanup]
    public void Cleanup()
    {
        _listener?.Dispose();
        _instrumentation.Dispose();
        _recovery.Dispose();
        _dataSource.Dispose();
    }

    /// <summary>Recovery's reusable read-buffer copy, ownership handoff, grouping and native projection; no database I/O.</summary>
    [Benchmark]
    public int RecoveryProjection()
    {
        var batch = _recovery.PrepareRecovered(_records);
        batch.Sort();
        var bytes = 0;
        for (var i = 0; i < batch.Count; i++) bytes += batch.Rows[i].ToMessage().Body.Length;
        batch.Clear();
        return bytes;
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
            _created[i] = _batch.Rows[i].CreatedAt;
            _claims[i] = new(_batch.Rows[i].Id, Guid.NewGuid());
            _messages[i] = _batch.Rows[i].ToMessage();
        }
        var live = PostgreSqlDirectDispatcher.CompactClaims(_messages, _claims, _claimed, _created);
        var bytes = 0;
        for (var i = 0; i < live; i++)
        {
            _instrumentation.RecordOutboxDispatchLag(_now - _created[i]);
            bytes += _messages[i].Body.Length;
        }
        _instrumentation.AddOutboxPending(-live);
        Array.Clear(_messages);
        Array.Clear(_claims);
        _batch.Clear();
        return bytes;
    }
}
