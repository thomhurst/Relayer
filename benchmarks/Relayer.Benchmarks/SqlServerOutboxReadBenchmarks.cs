using System.Buffers;
using System.Data;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Relayer.Outbox.PostgreSql;
using Relayer.Outbox.SqlServer;

namespace Relayer.Benchmarks;

/// <summary>Real SQL Server bounded-read scaling, including native I/O and header decoding.</summary>
/// <remarks>Set RELAYER_SQLSERVER_BENCHMARK_CONNECTION to an isolated database with DDL permissions.</remarks>
[MemoryDiagnoser]
public class SqlServerOutboxReadBenchmarks
{
    private string _connectionString = null!;
    private SqlServerOutboxStore _store = null!;
    private OutboxRecord[] _rows = null!;
    private readonly DateTimeOffset _now = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private readonly SqlServerOutboxOptions _options = new() { TablePrefix = "benchmark_" + Guid.NewGuid().ToString("N") + "_" };

    [Params(1, 64)]
    public int Rows { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _connectionString = Environment.GetEnvironmentVariable("RELAYER_SQLSERVER_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_SQLSERVER_BENCHMARK_CONNECTION to an isolated SQL Server database.");
        _store = new(_connectionString, _options);
        _rows = new OutboxRecord[Rows];
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using (var schema = new SqlCommand(SqlServerOutboxSchema.CreateScript(_options), connection))
            await schema.ExecuteNonQueryAsync();
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, [new("ce_id", new("01900000-0000-7000-8000-000000000001")), new("traceparent", new("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))]);
        for (var i = 0; i < Rows; i++)
        {
            using var insert = new SqlCommand($"INSERT INTO [dbo].[{_options.TablePrefix}outbox] (id, transport, destination, message_type, headers, body, created_at) VALUES (@id, 'events', 'orders', 'order.placed.v1', @headers, @body, @now)", connection);
            insert.Parameters.Add("@id", SqlDbType.Binary, 16).Value = Guid.CreateVersion7().ToByteArray(bigEndian: true);
            insert.Parameters.Add("@headers", SqlDbType.VarBinary, -1).Value = headers.WrittenSpan.ToArray();
            insert.Parameters.Add("@body", SqlDbType.VarBinary, -1).Value = new byte[1024];
            insert.Parameters.Add("@now", SqlDbType.DateTimeOffset).Value = _now;
            await insert.ExecuteNonQueryAsync();
        }
        if (await _store.ReadPendingAsync(new(null, _now), _rows, default) != Rows)
            throw new InvalidOperationException("Benchmark fixture must return the configured row count.");
    }

    [Benchmark]
    public ValueTask<int> ReadPending() => _store.ReadPendingAsync(new(null, _now), _rows, default);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"DROP TABLE IF EXISTS [dbo].[{_options.TablePrefix}outbox]", connection);
        await command.ExecuteNonQueryAsync();
    }
}
