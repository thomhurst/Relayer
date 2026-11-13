using BenchmarkDotNet.Attributes;
using Npgsql;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Compares explicit-clock and database-clock fenced batches in identical rollback transactions.</summary>
[MemoryDiagnoser]
public class PostgreSqlDirectClockBenchmarks
{
    private NpgsqlDataSource _source = null!;
    private NpgsqlConnection _connection = null!;
    private PostgreSqlOutboxStore _store = null!;
    private readonly PostgreSqlOutboxOptions _options = new() { TablePrefix = "clock_" + Guid.NewGuid().ToString("N")[..12] + "_" };
    private OutboxClaim[] _claims = [];
    private bool[] _claimed = [];
    private string _table = null!;

    [Params(1, 100)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _source = NpgsqlDataSource.Create(Environment.GetEnvironmentVariable("RELAYER_POSTGRES_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_POSTGRES_BENCHMARK_CONNECTION to an isolated PostgreSQL database."));
        _connection = await _source.OpenConnectionAsync();
        _store = new(_source, _options);
        _table = "public." + PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "outbox");
        await using (var schema = new NpgsqlCommand(PostgreSqlOutboxSchema.CreateScript(_options), _connection))
            await schema.ExecuteNonQueryAsync();
        _claims = new OutboxClaim[MessageCount];
        _claimed = new bool[MessageCount];
        var ids = new Guid[MessageCount];
        for (var i = 0; i < MessageCount; i++) _claims[i] = new(ids[i] = Guid.CreateVersion7(), Guid.NewGuid());
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO {_table} (id, transport, destination, message_type, body, headers, created_at)
            SELECT id, 'events', 'work', 'probe', ''::bytea, ''::bytea, clock_timestamp() FROM unnest(@ids) AS row(id)
            """, _connection);
        insert.Parameters.AddWithValue("ids", ids);
        await insert.ExecuteNonQueryAsync();
    }

    [Benchmark(Baseline = true)]
    public async ValueTask ExplicitClock()
    {
        await using var transaction = await _connection.BeginTransactionAsync();
        _store.UseDispatchTransaction(_connection, transaction);
        var now = DateTimeOffset.UtcNow;
        var expires = now.AddMinutes(1);
        await _store.ClaimBatchAsync(_claims, _claimed, now, expires, default);
        if (expires <= DateTimeOffset.UtcNow || !_claimed[0]) throw new InvalidOperationException("Benchmark claim expired.");
        if (await _store.CompleteBatchAsync(_claims, DateTimeOffset.UtcNow, default) != MessageCount)
            throw new InvalidOperationException("Benchmark completion failed.");
        await transaction.RollbackAsync();
        _store.UseDispatchTransaction(null, null);
    }

    [Benchmark]
    public async ValueTask DatabaseClock()
    {
        await using var transaction = await _connection.BeginTransactionAsync();
        _store.UseDispatchTransaction(_connection, transaction);
        var started = TimeProvider.System.GetTimestamp();
        await _store.ClaimDirectBatchAsync(_claims, _claimed, TimeSpan.FromMinutes(1), default);
        if (TimeProvider.System.GetElapsedTime(started) >= TimeSpan.FromMinutes(1) || !_claimed[0])
            throw new InvalidOperationException("Benchmark claim expired.");
        if (await _store.CompleteDirectBatchAsync(_claims, default) != MessageCount)
            throw new InvalidOperationException("Benchmark completion failed.");
        await transaction.RollbackAsync();
        _store.UseDispatchTransaction(null, null);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        foreach (var suffix in new[] { "outbox", "outbox_dead", "inbox", "leases" })
        {
            await using var command = new NpgsqlCommand($"DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + suffix)}", _connection);
            await command.ExecuteNonQueryAsync();
        }
        await _connection.DisposeAsync();
        await _source.DisposeAsync();
    }
}
