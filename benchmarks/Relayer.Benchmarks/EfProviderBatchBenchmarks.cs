using Relayer.Transports;
using BenchmarkDotNet.Attributes;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Isolates provider dispatch overhead against the same native reusable batch, without database I/O per iteration.</summary>
[MemoryDiagnoser]
public class EfProviderBatchBenchmarks
{
    private NpgsqlConnection _connection = null!;
    private NpgsqlTransaction _transaction = null!;
    private PostgreSqlOutboxBatch _native = null!;
    private IOutboxBatch _provider = null!;
    private BufferedOutboxMessage _row = null!;
    private BufferedOutboxMessage[] _rows = null!;
    private string _insert = null!;

    [Params(1, 256)]
    public int Count { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _connection = new NpgsqlConnection(Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_POSTGRES")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL benchmark database."));
        await _connection.OpenAsync();
        _transaction = await _connection.BeginTransactionAsync();
        var configuration = new PostgreSqlOutboxConfiguration(new());
        _insert = configuration.InsertSql;
        _native = new();
        _provider = configuration.CreateBatch();
        _row = new();
        _row.Copy(new(Guid.CreateVersion7(), "events", "benchmark;v=1",
            new OutgoingMessage("orders", new byte[256], default), DateTimeOffset.UtcNow), 32);
        _rows = new BufferedOutboxMessage[Count];
        Array.Fill(_rows, _row);
        NativeBuffer();
        ProviderBuffer();
    }

    [Benchmark(Baseline = true)]
    public void NativeBuffer()
    {
        _native.Prepare(_transaction, _insert);
        for (var i = 0; i < _rows.Length; i++) _native.Add(_rows[i]);
    }

    [Benchmark]
    public void ProviderBuffer()
    {
        _provider.Prepare(_transaction, _insert);
        _provider.AddRange(_rows);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _native.Dispose();
        _provider.Dispose();
        _row.Clear();
        await _transaction.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
