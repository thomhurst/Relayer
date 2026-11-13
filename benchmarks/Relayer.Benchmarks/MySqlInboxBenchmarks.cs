using Relayer.Transports;
using System.Data;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;
using Relayer.Outbox.MySql;

namespace Relayer.Benchmarks;

/// <summary>Compares the consumer boundary with native InnoDB insert/commit. Requires RELAYER_BENCHMARK_MYSQL.</summary>
[MemoryDiagnoser]
public class MySqlInboxBenchmarks
{
    private MySqlDataSource _source = null!;
    private ServiceProvider _provider = null!;
    private MySqlConsumer _consumer = null!;
    private Context _context = null!;
    private string _prefix = null!;
    private string _insert = null!;
    private readonly string _duplicate = Guid.CreateVersion7().ToString("D");
    private readonly byte[] _id = new byte[16];
    private readonly byte[] _identity = "benchmark"u8.ToArray();

    [Params(false, true)]
    public bool Duplicate { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _source = new(Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_MYSQL")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_MYSQL to a disposable MySQL 8.4+ database."));
        _prefix = "inbox_bench_" + Guid.NewGuid().ToString("N") + "_";
        var options = new MySqlOutboxOptions { TablePrefix = _prefix };
        var services = new ServiceCollection();
        services.AddSingleton<RelayerInstrumentation>();
        services.AddRelayerMySqlOutbox(options);
        _provider = services.BuildServiceProvider();
        await using (var create = _source.CreateCommand(MySqlOutboxSchema.CreateScript(options)))
            await create.ExecuteNonQueryAsync();
        _insert = $"INSERT IGNORE INTO {MySqlOutboxSchema.InboxTable(options)} (consumer, message_id) VALUES (@consumer, @id)";
        _consumer = new(_source, "benchmark", _identity, true, IsolationLevel.Unspecified,
            _provider.GetRequiredService<RelayerInstrumentation>());
        _context = new() { Id = _duplicate };
        var duplicate = Duplicate;
        Duplicate = true;
        await RelayerInbox();
        Duplicate = duplicate;
    }

    [Benchmark(Baseline = true)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask NativeInsert()
    {
        Guid.Parse(Duplicate ? _duplicate : Guid.CreateVersion7().ToString("D")).TryWriteBytes(_id, bigEndian: true, out _);
        await using var connection = await _source.OpenConnectionAsync().ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);
        await using var command = new MySqlCommand(_insert, connection, transaction);
        command.Parameters.Add(new("consumer", MySqlDbType.VarBinary) { Value = _identity });
        command.Parameters.Add(new("id", MySqlDbType.Binary) { Value = _id });
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        await transaction.CommitAsync().ConfigureAwait(false);
    }

    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask RelayerInbox()
    {
        await using var scope = _provider.CreateAsyncScope();
        _context.ServiceProvider = scope.ServiceProvider;
        _context.Id = Duplicate ? _duplicate : Guid.CreateVersion7().ToString("D");
        try { await _consumer.InvokeAsync(_context, static _ => default).ConfigureAwait(false); }
        finally { _context.ServiceProvider = null!; }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await using (var drop = _source.CreateCommand($"DROP TABLE `{_prefix}inbox`, `{_prefix}outbox`"))
            await drop.ExecuteNonQueryAsync();
        await _provider.DisposeAsync();
        await _source.DisposeAsync();
    }

    private sealed class Context : MessageContext
    {
        internal IServiceProvider ServiceProvider { get; set; } = null!;
        internal string Id { get; set; } = "";
        public override string MessageId => Id;
        public override MessageType MessageType => new("benchmark", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => ServiceProvider;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
