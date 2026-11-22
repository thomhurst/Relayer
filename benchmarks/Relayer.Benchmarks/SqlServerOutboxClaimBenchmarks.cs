using System.Data;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Relayer.Outbox.SqlServer;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Measures unordered database-clock claims and completions in native SqlBatch RPCs.</summary>
[MemoryDiagnoser]
public class SqlServerOutboxClaimBenchmarks
{
    private readonly SqlServerOutboxOptions _options = new() { TablePrefix = "claim_" + Guid.NewGuid().ToString("N") + "_" };
    private SqlConnection _connection = null!;
    private SqlServerOutboxStore _store = null!;
    private string _claimSql = null!;
    private OutboxClaim[] _claims = [];
    private bool[] _claimed = [];

    [Params(1, 100)]
    public int Rows
    {
        get; set;
    }

    [GlobalSetup]
    public async Task Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("RELAYER_SQLSERVER_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_SQLSERVER_BENCHMARK_CONNECTION to an isolated SQL Server database.");
        _connection = new(connectionString);
        await _connection.OpenAsync();
        await using var schema = new SqlCommand(SqlServerOutboxSchema.CreateScript(_options), _connection);
        await schema.ExecuteNonQueryAsync();

        _store = new(connectionString, _options);
        _claimSql = _store.CreateDispatchClaimSql(TimeSpan.FromMinutes(1));
        _claims = new OutboxClaim[Rows];
        _claimed = new bool[Rows];
        await using var insert = new SqlCommand($"""
            INSERT INTO {SqlServerOutboxSchema.Table(_options)}
                (id, transport, destination, message_type, body, headers, created_at, deliver_at)
            VALUES (@id, N'events', N'orders', N'probe', 0x, 0x, SYSUTCDATETIME(), SYSUTCDATETIME())
            """, _connection);
        var idParameter = insert.Parameters.Add("@id", SqlDbType.Binary, 16);

        for (var i = 0; i < Rows; i++)
        {
            var id = Guid.CreateVersion7();
            _claims[i] = new(id, Guid.NewGuid());
            idParameter.Value = id.ToByteArray(bigEndian: true);
            await insert.ExecuteNonQueryAsync();
        }
    }

    // Rollback restores the seed without timing inserts or changing the fenced SQL.
    [Benchmark]
    public async ValueTask<long> ClaimAndComplete()
    {
        await using var transaction = (SqlTransaction)await _connection.BeginTransactionAsync();
        _store.UseDispatchTransaction(_connection, transaction);
        await _store.ClaimBatchAsync(_claims, _claimed, _claimSql, default);
        var completed = await _store.CompleteBatchAsync(_claims, default);
        await transaction.RollbackAsync();
        _store.UseDispatchTransaction(null, null);

        return completed == Rows ? completed : throw new InvalidOperationException("Incomplete claim benchmark batch.");
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await using var drop = new SqlCommand($"""
            DROP TABLE IF EXISTS {SqlServerOutboxSchema.Table(_options)}, {SqlServerOutboxSchema.InboxTable(_options)},
                {SqlServerOutboxSchema.LeaseTable(_options)}, {SqlServerOutboxSchema.OrderedLeaseTable(_options)},
                {SqlServerOutboxSchema.DeadTable(_options)};
            DROP SEQUENCE IF EXISTS {SqlServerOutboxSchema.Sequence(_options)};
            """, _connection);
        await drop.ExecuteNonQueryAsync();
        await _connection.DisposeAsync();
    }
}
