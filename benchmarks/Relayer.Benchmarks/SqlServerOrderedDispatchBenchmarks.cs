using System.Data;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer.Outbox.SqlServer;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Real SQL Server lane ownership, ordered reads, reusable decoding and fenced outcomes per batch.</summary>
/// <remarks>Includes native SqlClient I/O and allocations; run against an isolated database. Setup is outside measurement.</remarks>
[MemoryDiagnoser]
public class SqlServerOrderedDispatchBenchmarks
{
    private readonly SqlServerOutboxOptions _options = new() { Lanes = 1, TablePrefix = "ordered_" + Guid.NewGuid().ToString("N") + "_" };
    private string _connectionString = null!;
    private string _table = null!;
    private DataTable _rows = null!;
    private SqlServerOrderedDispatcher _worker = null!;

    [Params(1, 64)]
    public int Rows { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _connectionString = Environment.GetEnvironmentVariable("RELAYER_SQLSERVER_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_SQLSERVER_BENCHMARK_CONNECTION to an isolated SQL Server database.");
        _table = SqlServerOutboxSchema.Table(_options);
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var schema = new SqlCommand(SqlServerOutboxSchema.CreateScript(_options), connection);
        await schema.ExecuteNonQueryAsync();
        _rows = new DataTable();
        _rows.Columns.Add("id", typeof(byte[]));
        _rows.Columns.Add("lane", typeof(int));
        _rows.Columns.Add("ordering_key", typeof(string));
        _rows.Columns.Add("transport", typeof(string));
        _rows.Columns.Add("destination", typeof(string));
        _rows.Columns.Add("message_type", typeof(string));
        _rows.Columns.Add("headers", typeof(byte[]));
        _rows.Columns.Add("body", typeof(byte[]));
        _rows.Columns.Add("created_at", typeof(DateTimeOffset));
        for (var i = 0; i < Rows; i++)
            _rows.Rows.Add(Guid.CreateVersion7().ToByteArray(bigEndian: true), 0, "customer", "events", "orders", "probe;v=1",
                new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 }, new byte[1024], DateTimeOffset.UtcNow);
        _worker = new(_connectionString, new() { TablePrefix = _options.TablePrefix, Lanes = 1, BatchSize = 64 },
            [new NoOpTransport()], NullLogger<SqlServerOrderedDispatcher>.Instance);
        Insert();
        if (await DispatchLane() != Rows) throw new InvalidOperationException("Incomplete ordered benchmark batch.");
    }

    [IterationSetup]
    public void Insert()
    {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var copy = new SqlBulkCopy(connection) { DestinationTableName = _table };
        foreach (DataColumn column in _rows.Columns) copy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        copy.WriteToServer(_rows);
    }

    [Benchmark]
    public ValueTask<int> DispatchLane() => _worker.DispatchLaneAsync(0, default);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _worker.Dispose();
        _rows.Dispose();
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        await using var drop = new SqlCommand($"DROP TABLE IF EXISTS {_table}, [dbo].[{_options.TablePrefix}inbox], [dbo].[{_options.TablePrefix}leases]; DROP SEQUENCE IF EXISTS [dbo].[{_options.TablePrefix}outbox_sequence]", connection);
        await drop.ExecuteNonQueryAsync();
    }

    private sealed class NoOpTransport : ITransport
    {
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken token) => ValueTask.CompletedTask;
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken token) => Task.CompletedTask;
        public Task StopAsync(CancellationToken token) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
