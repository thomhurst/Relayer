using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>
/// Compares CDC's bounded traversal with one polling recovery pass, including native database I/O.
/// Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL database. Native WAL decoding is excluded.
/// </summary>
[MemoryDiagnoser]
public class PostgreSqlChangeCaptureBenchmarks
{
    private NpgsqlDataSource _source = null!;
    private PostgreSqlMaintenanceWorker _worker = null!;
    private readonly string _schema = "cdc_benchmark_" + Guid.NewGuid().ToString("N");
    private readonly CountingTransport _transport = new();
    private byte[] _headers = [];

    [Params(1, 100)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_POSTGRES")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL database.");
        _source = NpgsqlDataSource.Create(connectionString);
        using var schema = _source.CreateCommand(PostgreSqlOutboxSchema.CreateScript(new() { Schema = _schema }));
        schema.ExecuteNonQuery();
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, []);
        _headers = headers.WrittenMemory.ToArray();
        _worker = new(_source, new() { Schema = _schema, RecoveryDelay = TimeSpan.Zero, BatchSize = 100 },
            [_transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        Populate();
        _worker.DispatchCommittedAsync(default).AsTask().GetAwaiter().GetResult();
    }

    [IterationSetup]
    public void Populate()
    {
        // Reset the polling keyset boundary and warm reusable native/store buffers outside measurement.
        _worker.DispatchCommittedAsync(default).AsTask().GetAwaiter().GetResult();
        _transport.Count = 0;
        using var command = _source.CreateCommand($"TRUNCATE {_schema}.relayer_outbox, {_schema}.relayer_leases; INSERT INTO {_schema}.relayer_outbox (id, transport, destination, message_type, headers, body, created_at) SELECT gen_random_uuid(), 'events', 'orders', 'order;v=1', @headers, @body, clock_timestamp() FROM generate_series(1, @count)");
        command.Parameters.AddWithValue("headers", _headers);
        command.Parameters.AddWithValue("body", new byte[256]);
        command.Parameters.AddWithValue("count", MessageCount);
        command.ExecuteNonQuery();
    }

    [Benchmark(Baseline = true)]
    public ValueTask<int> PollingRecovery() => _worker.SweepAsync(default);

    [Benchmark]
    public ValueTask CdcRecovery() => _worker.DispatchCommittedAsync(default);

    [IterationCleanup]
    public void Verify()
    {
        if (_transport.Count != MessageCount) throw new InvalidOperationException("Recovery did not send every seeded row.");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _worker?.Dispose();
        if (_source is null) return;
        try
        {
            using var command = _source.CreateCommand($"DROP SCHEMA {_schema} CASCADE");
            command.ExecuteNonQuery();
        }
        finally { _source.Dispose(); }
    }

    private sealed class CountingTransport : ITransport
    {
        internal int Count;
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 100 };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Count++; return default; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) { Count += messages.Length; return default; }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
