using System.Data;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.SqlServer;

namespace Relayer.Benchmarks;

/// <summary>Attributes reusable outbox buffer cost without native database I/O.</summary>
[MemoryDiagnoser]
public class SqlServerOutboxBufferBenchmarks
{
    private readonly SqlServerBufferedMessage _row = new();
    private OutboxRecord _record;

    [Params(256, 4096)]
    public int BodySize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _record = new(Guid.CreateVersion7(), "events", "benchmark.order;v=1",
            new OutgoingMessage("orders", new byte[BodySize],
                new KeyValuePair<string, WireString>[] { new("ce_type", new WireString("benchmark.order;v=1")), new("custom", new WireString(new byte[] { 0, 255 })) },
                new WireString("customer-é"u8.ToArray())), DateTimeOffset.UtcNow);
        CopyAndRelease();
    }

    [Benchmark]
    public int CopyAndRelease()
    {
        _row.Copy(_record, 32);
        var size = ((MemoryStream)_row.Values[6]).Length + ((MemoryStream)_row.Values[7]).Length;
        _row.Clear();
        return checked((int)size);
    }
}

/// <summary>Compares native bulk-copy commits with scoped buffered commits at realistic batch sizes.</summary>
/// <remarks>Results include SqlClient I/O and allocations. One invocation is a complete batch, including deletion of the previous batch.</remarks>
[MemoryDiagnoser]
public class SqlServerOutboxCommitBenchmarks
{
    private string _connectionString = null!;
    private readonly SqlServerOutboxOptions _options = new() { TablePrefix = "commit_" + Guid.NewGuid().ToString("N") + "_" };
    private string _table = null!;
    private DataTable _nativeRows = null!;
    private OutboxRecord[] _records = null!;
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private SqlServerOutbox _outbox = null!;

    [Params(1, 64)]
    public int Rows { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _connectionString = Environment.GetEnvironmentVariable("RELAYER_SQLSERVER_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_SQLSERVER_BENCHMARK_CONNECTION to an isolated SQL Server database.");
        _table = "[dbo].[" + _options.TablePrefix + "outbox]";
        _provider = new ServiceCollection().AddRelayerSqlServerOutbox(_options).BuildServiceProvider();
        _scope = _provider.CreateScope();
        _outbox = _scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        _nativeRows = new DataTable();
        _nativeRows.Columns.Add("id", typeof(byte[]));
        _nativeRows.Columns.Add("transport", typeof(string));
        _nativeRows.Columns.Add("destination", typeof(string));
        _nativeRows.Columns.Add("message_type", typeof(string));
        _nativeRows.Columns.Add("headers", typeof(byte[]));
        _nativeRows.Columns.Add("body", typeof(byte[]));
        _nativeRows.Columns.Add("created_at", typeof(DateTimeOffset));
        _records = new OutboxRecord[Rows];
        var now = DateTimeOffset.UtcNow;
        var body = new byte[1024];
        for (var i = 0; i < Rows; i++)
        {
            var id = Guid.CreateVersion7();
            _records[i] = new(id, "events", "benchmark.order;v=1", new OutgoingMessage("orders", body), now);
            _nativeRows.Rows.Add(id.ToByteArray(bigEndian: true), "events", "orders", "benchmark.order;v=1",
                new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 }, body, now);
        }
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var schema = new SqlCommand(SqlServerOutboxSchema.CreateScript(_options), connection);
        await schema.ExecuteNonQueryAsync();
        // Warm reusable buffers and verify both writers produce the complete batch.
        await NativeBulkCopy();
        await BufferedCommit();
        await using var count = new SqlCommand($"SELECT COUNT_BIG(*) FROM {_table}", connection);
        if ((long)(await count.ExecuteScalarAsync())! != Rows) throw new InvalidOperationException("Incomplete benchmark batch.");
    }

    [Benchmark(Baseline = true)]
    public async Task NativeBulkCopy()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await using (var delete = new SqlCommand($"DELETE FROM {_table}", connection, transaction))
            await delete.ExecuteNonQueryAsync();
        using var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers, transaction)
        { DestinationTableName = _table };
        foreach (DataColumn column in _nativeRows.Columns) copy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        await copy.WriteToServerAsync(_nativeRows);
        await transaction.CommitAsync();
    }

    [Benchmark]
    public async Task BufferedCommit()
    {
        await using var transaction = await _outbox.BeginTransactionAsync(_connectionString);
        await using (var delete = transaction.Connection!.CreateCommand())
        {
            delete.CommandText = $"DELETE FROM {_table}";
            await delete.ExecuteNonQueryAsync();
        }
        for (var i = 0; i < _records.Length; i++) await _outbox.EnlistAsync(_records[i], default);
        await transaction.CommitAsync();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var drop = new SqlCommand($"DROP TABLE IF EXISTS {_table}", connection);
        await drop.ExecuteNonQueryAsync();
        _nativeRows.Dispose();
        _scope.Dispose();
        await _provider.DisposeAsync();
    }
}
