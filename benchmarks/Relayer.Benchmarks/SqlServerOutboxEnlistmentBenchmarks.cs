using Relayer.Transports;
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
/// <remarks>Results include SqlClient I/O and allocations. One invocation is a complete batch, including truncation of the previous batch.</remarks>
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

    [Params(1, 64, 7000)]
    public int Rows { get; set; }

    [Params(false, true)]
    public bool Ordered { get; set; }

    [Params(false, true)]
    public bool InsertTrigger { get; set; }

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
        _nativeRows.Columns.Add("lane", typeof(int));
        _nativeRows.Columns.Add("ordering_key", typeof(string));
        _records = new OutboxRecord[Rows];
        var now = DateTimeOffset.UtcNow;
        var body = new byte[1024];
        for (var i = 0; i < Rows; i++)
        {
            var id = Guid.CreateVersion7();
            _records[i] = new(id, "events", "benchmark.order;v=1", new OutgoingMessage("orders", body, orderingKey: Ordered ? new WireString("customer") : null), now);
            _nativeRows.Rows.Add(id.ToByteArray(bigEndian: true), "events", "orders", "benchmark.order;v=1",
                new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 }, body, now, Ordered ? 0 : DBNull.Value, Ordered ? "customer" : DBNull.Value);
        }
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var schema = new SqlCommand(SqlServerOutboxSchema.CreateScript(_options), connection);
        await schema.ExecuteNonQueryAsync();
        if (InsertTrigger)
        {
            await using var trigger = new SqlCommand($"""
                CREATE TABLE [dbo].[{_options.TablePrefix}batch_log] (rows_inserted bigint NOT NULL);
                EXEC(N'CREATE TRIGGER [dbo].[{_options.TablePrefix}insert_log] ON {_table}
                    AFTER INSERT AS BEGIN SET NOCOUNT ON;
                    INSERT INTO [dbo].[{_options.TablePrefix}batch_log] SELECT COUNT_BIG(*) FROM inserted; END');
                """, connection);
            await trigger.ExecuteNonQueryAsync();
        }
        // Warm reusable buffers and verify both writers produce the complete batch.
        await NativeBulkCopy();
        await BufferedCommit();
        await using var count = new SqlCommand($"SELECT COUNT_BIG(*) FROM {_table}", connection);
        if ((long)(await count.ExecuteScalarAsync())! != Rows) throw new InvalidOperationException("Incomplete benchmark batch.");
    }

    [Benchmark(Baseline = true)]
    public Task NativeBulkCopy() => NativeBulkCopyCore(streaming: false);

    [Benchmark]
    public Task NativeStreamingBulkCopy() => NativeBulkCopyCore(streaming: true);

    private async Task NativeBulkCopyCore(bool streaming)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await using (var delete = new SqlCommand($"TRUNCATE TABLE {_table}", connection, transaction))
            await delete.ExecuteNonQueryAsync();
        using var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers, transaction)
        { DestinationTableName = _table, EnableStreaming = streaming };
        foreach (DataColumn column in _nativeRows.Columns) copy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        if (streaming)
        {
            using var reader = _nativeRows.CreateDataReader();
            await copy.WriteToServerAsync(reader);
        }
        else await copy.WriteToServerAsync(_nativeRows);
        await transaction.CommitAsync();
    }

    [Benchmark]
    public async Task BufferedCommit()
    {
        await using var transaction = await _outbox.BeginTransactionAsync(_connectionString);
        await using (var delete = transaction.Connection!.CreateCommand())
        {
            delete.CommandText = $"TRUNCATE TABLE {_table}";
            await delete.ExecuteNonQueryAsync();
        }
        for (var i = 0; i < _records.Length; i++) await _outbox.EnlistAsync(_records[i], default);
        await transaction.CommitAsync();
    }

    [IterationCleanup]
    public void VerifyBatch()
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var count = new SqlCommand($"SELECT COUNT_BIG(*) FROM {_table}", connection);
        if ((long)count.ExecuteScalar()! != Rows) throw new InvalidOperationException("Incomplete benchmark batch.");
        if (InsertTrigger)
        {
            using var log = new SqlCommand($"SELECT MIN(rows_inserted) FROM [dbo].[{_options.TablePrefix}batch_log]", connection);
            if ((long)log.ExecuteScalar()! != Rows) throw new InvalidOperationException("Trigger did not receive a complete batch.");
            using var clear = new SqlCommand($"TRUNCATE TABLE [dbo].[{_options.TablePrefix}batch_log]", connection);
            clear.ExecuteNonQuery();
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var drop = new SqlCommand($"DROP TABLE IF EXISTS [dbo].[{_options.TablePrefix}batch_log], {_table}, [dbo].[{_options.TablePrefix}inbox], [dbo].[{_options.TablePrefix}leases], [dbo].[{_options.TablePrefix}ordered_leases], [dbo].[{_options.TablePrefix}outbox_dead]; DROP SEQUENCE IF EXISTS [dbo].[{_options.TablePrefix}outbox_sequence]", connection);
        await drop.ExecuteNonQueryAsync();
        _nativeRows.Dispose();
        _scope.Dispose();
        await _provider.DisposeAsync();
    }
}
