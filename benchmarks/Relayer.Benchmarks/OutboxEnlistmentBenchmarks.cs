using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.EntityFrameworkCore;

namespace Relayer.Benchmarks;

/// <summary>Enlists a serialized send before an implicit save, then releases reusable buffers without database I/O.</summary>
[MemoryDiagnoser]
public class OutboxEnlistmentBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private DbContext _context = null!;
    private IDisposable _selection = null!;
    private IOutboxEnlistment _outbox = null!;
    private OutboxContext _state = null!;
    private OutboxRecord _record;

    [Params(256, 4096)]
    public int BodySize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _provider = new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox().BuildServiceProvider();
        _scope = _provider.CreateScope();
        var options = new DbContextOptionsBuilder();
        options.UseNpgsql("Host=unused").AddRelayerOutbox(_scope.ServiceProvider);
        _context = new DbContext(options.Options);
        _selection = _context.EnlistRelayerOutbox();
        _outbox = _scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        _state = ((EntityFrameworkCoreOutbox)_outbox).Get(_context, new OutboxConfiguration(new()));
        KeyValuePair<string, WireString>[] headers =
        [
            new("ce_id", new WireString("00000000-0000-7000-8000-000000000001"u8.ToArray())),
            new("ce_type", new WireString("benchmark.order;v=1")),
            new("traceparent", new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))
        ];
        _record = new(Guid.CreateVersion7(), "events", "benchmark.order;v=1",
            new OutgoingMessage("orders", new byte[BodySize], headers, new WireString("customer-42")), DateTimeOffset.UtcNow);
        _state.Add(_record);
        _state.Clear();
    }

    [Benchmark]
    public async ValueTask<int> EnlistAndRelease()
    {
        if (!_outbox.IsActive) throw new InvalidOperationException("No selected outbox context.");
        await _outbox.EnlistAsync(_record, default).ConfigureAwait(false);
        var row = _state.Rows[0];
        var length = row.Body.Length + row.Headers.Length;
        _state.Clear();
        return length;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _selection.Dispose();
        _context.Dispose();
        _scope.Dispose();
        _provider.Dispose();
    }
}
