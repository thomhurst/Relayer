using BenchmarkDotNet.Attributes;
using MySqlConnector;
using Relayer.Outbox.MySql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Measures recovery claims with and without a pool checkout. Requires RELAYER_BENCHMARK_MYSQL.</summary>
[MemoryDiagnoser]
public class MySqlRecoveryConnectionBenchmarks
{
    private readonly bool[] _claimed = new bool[1];
    private readonly OutboxClaim[] _claims = [new(Guid.CreateVersion7(), Guid.NewGuid())];
    private MySqlDataSource _source = null!;
    private MySqlConnection _admission = null!;
    private MySqlOutboxStore _store = null!;
    private MySqlOutboxOptions _options = null!;
    private DateTimeOffset _now;

    [GlobalSetup]
    public async Task Setup()
    {
        _source = new(Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_MYSQL")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_MYSQL to a disposable MySQL 8.4+ database."));
        _options = new()
        {
            TablePrefix = "recover_bench_" + Guid.NewGuid().ToString("N") + "_"
        };
        _store = new(_source, _options);
        _now = DateTimeOffset.UtcNow;

        await using (var create = _source.CreateCommand(MySqlOutboxSchema.CreateScript(_options)))
        {
            await create.ExecuteNonQueryAsync();
        }

        _admission = await _source.OpenConnectionAsync();
        await using var seed = new MySqlCommand($"""
            INSERT INTO {MySqlOutboxSchema.Table(_options)} (id, transport, destination, message_type, headers, body, created_at)
            VALUES (@id, 'events', 'orders', 'order;v=1', X'', X'01', UTC_TIMESTAMP(6))
            """, _admission);
        seed.Parameters.AddWithValue("id", _claims[0].MessageId.ToByteArray(bigEndian: true));
        await seed.ExecuteNonQueryAsync();
    }

    [Benchmark(Baseline = true)]
    public ValueTask<bool> SeparateConnection()
    {
        return ClaimAsync(null);
    }

    [Benchmark]
    public ValueTask<bool> ReuseAdmissionConnection()
    {
        return ClaimAsync(_admission);
    }

    private async ValueTask<bool> ClaimAsync(MySqlConnection? connection)
    {
        _store.RecoveryConnection = connection;
        // Advance the claim clock so each invocation performs the same successful UPDATE.
        var now = _now;
        _now = now.AddSeconds(1);
        await _store.ClaimBatchAsync(_claims, _claimed, now, _now, default);
        return _claimed[0];
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _store.Dispose();
        await _admission.DisposeAsync();
        await using var drop = _source.CreateCommand($"""
            DROP TABLE IF EXISTS {MySqlOutboxSchema.Table(_options)}, {MySqlOutboxSchema.DeadTable(_options)},
                {MySqlOutboxSchema.LeaseTable(_options)}, {MySqlOutboxSchema.InboxTable(_options)}
            """);
        await drop.ExecuteNonQueryAsync();
        await _source.DisposeAsync();
    }
}
