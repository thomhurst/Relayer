using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

[MemoryDiagnoser]
public class EfOutboxSchedulingBenchmarks
{
    private NpgsqlDataSource _firstSource = null!;
    private NpgsqlDataSource _secondSource = null!;
    private DbContext _firstContext = null!;
    private DbContext _secondContext = null!;
    private OutboxContext _first = null!;
    private OutboxContext _second = null!;

    [GlobalSetup]
    public void Setup()
    {
        _firstSource = NpgsqlDataSource.Create("Host=unused;Database=first");
        _secondSource = NpgsqlDataSource.Create("Host=unused;Database=second");
        var options = new DbContextOptionsBuilder().UseNpgsql(_firstSource).Options;
        _firstContext = new DbContext(options);
        _secondContext = new DbContext(options);
        _secondContext.Database.SetDbConnection(_secondSource.CreateConnection(), contextOwnsConnection: true);

        var configuration = new PostgreSqlOutboxConfiguration(new(), workers: [
            new(_firstSource, null, "relayer_"),
            new(_secondSource, null, "relayer_")
        ]);
        _first = new OutboxContext(_firstContext, configuration);
        _second = new OutboxContext(_secondContext, configuration);

        if (!_first.SupportsScheduling || !_second.SupportsScheduling)
        {
            throw new InvalidOperationException("Both benchmark writers need matching scheduling workers.");
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = 2)]
    public bool SingleWriter()
    {
        return _first.SupportsScheduling & _first.SupportsScheduling;
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public bool AlternateWriters()
    {
        return _first.SupportsScheduling & _second.SupportsScheduling;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _secondContext.Dispose();
        _firstContext.Dispose();
        _secondSource.Dispose();
        _firstSource.Dispose();
    }
}
