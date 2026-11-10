using BenchmarkDotNet.Attributes;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Compares owned enqueue copies at representative payload sizes, without database or transaction setup costs.</summary>
[MemoryDiagnoser]
public class PostgreSqlEnqueueBufferBenchmarks
{
    private readonly BufferedOutboxMessage _row = new();
    private OutboxRecord _record;
    private OutboxTenantBinding? _tenant;

    [Params(false, true)]
    public bool BoundTenant { get; set; }

    [Params(256, 4096)]
    public int BodySize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        KeyValuePair<string, WireString>[] headers =
        [
            new("ce_id", new WireString("00000000-0000-7000-8000-000000000001"u8.ToArray())),
            new("ce_type", new WireString("benchmark.order;v=1")),
            new("traceparent", new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))
        ];
        if (BoundTenant)
        {
            var context = new TenantContextPool(1).Rent();
            context.Id = new("tenant-42"u8.ToArray());
            _tenant = new(context, "tenant-42");
            headers = [.. headers, new(TenantContext.HeaderName, context.Id)];
        }
        _record = new(Guid.CreateVersion7(), "events", "benchmark.order;v=1",
            new OutgoingMessage("orders", new byte[BodySize], headers, new WireString("customer-42"u8.ToArray())), DateTimeOffset.UtcNow);
        CopyAndRelease(); // Warm the reusable byte buffer.
    }

    [Benchmark]
    public int CopyAndRelease()
    {
        var record = _tenant is null ? _record : _tenant.Bind(_record);
        BufferedOutboxMessage.Validate(record);
        _row.Copy(record, 32);
        var length = _row.Body.Length + _row.Headers.Length + _row.OrderingKey!.Value.Length;
        _row.Clear();
        return length;
    }
}
