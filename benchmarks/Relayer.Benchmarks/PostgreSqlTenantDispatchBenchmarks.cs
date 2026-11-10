using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Measures catalog coordination over the same native recovery and ordered operations.
/// Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL database. Native I/O is included.</summary>
[MemoryDiagnoser]
public class PostgreSqlTenantDispatchBenchmarks
{
    private NpgsqlDataSource _source = null!;
    private PostgreSqlTenantDispatcher _catalog = null!;
    private PostgreSqlMaintenanceWorker[] _fixed = [];
    private PostgreSqlOrderedDispatcher[] _ordered = [];
    private PostgreSqlTenantStore[] _entries = [];
    private readonly string _prefix = "tenant_benchmark_" + Guid.NewGuid().ToString("N");
    private readonly CountingTransport _transport = new();
    private byte[] _headers = [];

    [Params(false, true)] public bool RetainSentMessages { get; set; }
    [Params(1, 16)] public int TenantCount { get; set; }
    [Params(1, 100)] public int MessagesPerTenant { get; set; }
    [Params(1, 4)] public int LaneCount { get; set; }
    [Params(1, 4)] public int DestinationCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connectionString = Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_POSTGRES")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL database.");
        _source = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString);
        var headers = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(headers, []);
        _headers = headers.WrittenMemory.ToArray();
        _entries = new PostgreSqlTenantStore[TenantCount];
        _fixed = new PostgreSqlMaintenanceWorker[TenantCount];
        _ordered = new PostgreSqlOrderedDispatcher[TenantCount];
        for (var i = 0; i < TenantCount; i++)
        {
            var schema = _prefix + "_" + i;
            var options = new PostgreSqlOutboxOptions { Schema = schema, Lanes = LaneCount, RetainSentMessages = RetainSentMessages };
            _entries[i] = new(i.ToString(), _source, options);
            using var create = _source.CreateCommand($"CREATE SCHEMA {schema};" + PostgreSqlOutboxSchema.CreateScript(options));
            create.ExecuteNonQuery();
            _fixed[i] = new(_source, new() { Schema = schema, RetainSentMessages = RetainSentMessages, RecoveryDelay = TimeSpan.Zero }, [_transport], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
            _ordered[i] = new(_source, new() { Schema = schema, Lanes = LaneCount, RetainSentMessages = RetainSentMessages }, [_transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance);
        }
        _catalog = new(new Catalog(_entries), new() { MaxConnections = 1, RecoveryDelay = TimeSpan.Zero,
            CleanupInterval = TimeSpan.FromDays(1) }, [_transport], NullLoggerFactory.Instance);
        _catalog.PassAsync(default).GetAwaiter().GetResult(); // Warm retention outside measurement.
    }

    [IterationSetup]
    public void Populate()
    {
        _transport.Count = 0;
        foreach (var entry in _entries)
        {
            var schema = entry.Options.Schema;
            using var command = _source.CreateCommand($"TRUNCATE {schema}.relayer_outbox, {schema}.relayer_outbox_sent, {schema}.relayer_leases; INSERT INTO {schema}.relayer_outbox (id,lane,ordering_key,transport,destination,message_type,headers,body,created_at) SELECT ('01900000-0000-7000-8000-' || lpad(to_hex(n),12,'0'))::uuid, CASE WHEN n % 2 = 0 THEN n % @lanes ELSE NULL END, CASE WHEN n % 2 = 0 THEN 'key-' || (n % @lanes)::text ELSE NULL END, 'events','orders-' || (n % @destinations)::text,'order;v=1',@headers,@body,clock_timestamp() FROM generate_series(1,@count) AS n");
            command.Parameters.AddWithValue("headers", _headers);
            command.Parameters.AddWithValue("body", new byte[256]);
            command.Parameters.AddWithValue("count", MessagesPerTenant);
            command.Parameters.AddWithValue("lanes", LaneCount);
            command.Parameters.AddWithValue("destinations", DestinationCount);
            command.ExecuteNonQuery();
        }
    }

    [Benchmark(Baseline = true)]
    public async Task FixedStorePasses()
    {
        for (var pass = 0; pass < 2; pass++)
            for (var i = 0; i < _fixed.Length; i++)
            {
                await _fixed[i].SweepAsync(default);
                await _ordered[i].DispatchPassAsync(TimeSpan.MaxValue, default);
            }
    }

    [Benchmark]
    public async Task CatalogPasses()
    {
        // Two bounded passes also reset the prior recovery boundary before traversing fresh UUIDs.
        await _catalog.PassAsync(default);
        await _catalog.PassAsync(default);
    }

    [IterationCleanup]
    public void Verify()
    {
        foreach (var entry in _entries)
        {
            using var command = _source.CreateCommand($"SELECT count(*) FROM {entry.Options.Schema}.relayer_outbox_sent");
            if ((long)command.ExecuteScalar()! != (RetainSentMessages ? MessagesPerTenant : 0))
                throw new InvalidOperationException("Sent-history capture did not match the configured completion policy.");
        }
        if (_transport.Count != TenantCount * MessagesPerTenant)
            throw new InvalidOperationException("Dispatch did not send every seeded row.");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _catalog?.ReleaseAllAsync().GetAwaiter().GetResult();
        _catalog?.Dispose();
        foreach (var worker in _fixed) worker.Dispose();
        foreach (var worker in _ordered) worker.Dispose();
        if (_source is null) return;
        try
        {
            foreach (var entry in _entries)
            {
                using var command = _source.CreateCommand($"DROP SCHEMA {entry.Options.Schema} CASCADE");
                command.ExecuteNonQuery();
            }
        }
        finally { _source.Dispose(); }
    }

    private sealed class Catalog(PostgreSqlTenantStore[] entries) : ITenantSource
    {
        public IReadOnlyList<PostgreSqlTenantStore> GetTenants() => entries;
    }
    private sealed class CountingTransport : ITransport
    {
        internal int Count;
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 100, OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Count++; return default; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) { Count += messages.Length; return default; }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
