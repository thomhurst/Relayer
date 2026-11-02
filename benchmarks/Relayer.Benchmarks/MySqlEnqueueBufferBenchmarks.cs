using BenchmarkDotNet.Attributes;
using MySqlConnector;
using Relayer.Outbox.MySql;

namespace Relayer.Benchmarks;

/// <summary>Owned enqueue copy and native parameter assignment, excluding database I/O and transaction setup.</summary>
[MemoryDiagnoser]
public class MySqlEnqueueBufferBenchmarks
{
    private readonly BufferedOutboxMessage _message = new();
    private MySqlOutboxBatch.Row _reused = null!;
    private MySqlBatchCommand _native = null!;
    private OutboxRecord _record;
    private OutboxRecord _shortRecord;
    private bool _short;

    [Params(256, 4096)]
    public int BodySize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _record = new(Guid.CreateVersion7(), "events", "benchmark.order;v=1",
            new OutgoingMessage("orders", new byte[BodySize], new KeyValuePair<string, WireString>[] { new("ce_type", new("benchmark.order;v=1")) },
                new WireString("customer-42"), DateTimeOffset.UtcNow.AddMinutes(1)), DateTimeOffset.UtcNow, TenantId: "tenant");
        _shortRecord = _record with { Message = new OutgoingMessage("orders", new byte[BodySize / 2]) };
        var sql = MySqlOutboxBatch.CreateInsertSql("`relayer_outbox`");
        _reused = new(sql);
        _native = new(sql);
        foreach (MySqlParameter parameter in _reused.Command.Parameters)
            _native.Parameters.Add(new MySqlParameter(parameter.ParameterName, parameter.MySqlDbType));
        ReusedParameters();
    }

    [Benchmark(Baseline = true)]
    public MySqlBatchCommand NativeObjectValues()
    {
        var record = NextRecord();
        BufferedOutboxMessage.Validate(record);
        _message.Copy(record, 64);
        _native.Parameters["id"].Value = _message.Id.ToByteArray(bigEndian: true);
        _native.Parameters["lane"].Value = _message.Lane;
        _native.Parameters["key"].Value = _message.OrderingKey;
        _native.Parameters["transport"].Value = _message.Transport;
        _native.Parameters["destination"].Value = _message.Destination;
        _native.Parameters["type"].Value = _message.MessageType;
        _native.Parameters["headers"].Value = _message.Headers;
        _native.Parameters["body"].Value = _message.Body;
        _native.Parameters["created"].Value = _message.CreatedAt;
        _native.Parameters["delivery"].Value = _message.DeliverAt;
        _native.Parameters["tenant"].Value = _message.TenantId;
        _message.Clear();
        return _native;
    }

    [Benchmark]
    public MySqlBatchCommand ReusedParameters()
    {
        var record = NextRecord();
        BufferedOutboxMessage.Validate(record);
        _message.Copy(record, 64);
        _reused.Set(_message);
        _reused.Clear();
        _message.Clear();
        return _reused.Command;
    }

    private OutboxRecord NextRecord()
    {
        _short = !_short;
        return _short ? _shortRecord : _record;
    }
}
