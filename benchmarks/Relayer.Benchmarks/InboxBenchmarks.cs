using Relayer.Transports;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Compares inbox middleware with the same native insert and EF transaction. Requires RELAYER_BENCHMARK_POSTGRES.</summary>
[MemoryDiagnoser]
public class InboxBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScope _scope = null!;
    private DbContext _db = null!;
    private Context _context = null!;
    private IConsumeMiddleware _middleware = null!;
    private string _insert = null!;
    private string _schema = null!;
    private readonly string _duplicate = Guid.CreateVersion7().ToString("D");

    [Params(false, true)]
    public bool Duplicate { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        var connection = Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_POSTGRES")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL benchmark database.");
        _schema = "inbox_bench_" + Guid.NewGuid().ToString("N");
        var options = new EntityFrameworkCoreOutboxOptions { Schema = _schema };
        var services = new ServiceCollection();
        services.AddDbContext<DbContext>(o => o.UseNpgsql(connection));
        services.AddSingleton<RelayerInstrumentation>();
        _provider = services.BuildServiceProvider();
        _scope = _provider.CreateScope();
        _db = _scope.ServiceProvider.GetRequiredService<DbContext>();
        await _db.Database.OpenConnectionAsync();
        await using (var create = new NpgsqlCommand(PostgreSqlOutboxSchema.CreateScript(new() { Schema = _schema }),
            (NpgsqlConnection)_db.Database.GetDbConnection()))
            await create.ExecuteNonQueryAsync();
        var configuration = new OutboxConfiguration(options);
        _insert = configuration.InboxInsertSql;
        _middleware = new EntityFrameworkCoreConsumer<DbContext>("benchmark", true, false, configuration,
            _provider.GetRequiredService<RelayerInstrumentation>());
        _context = new Context(_scope.ServiceProvider) { Id = _duplicate };
        await _middleware.InvokeAsync(_context, static _ => default);
    }

    [Benchmark(Baseline = true)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask NativeInsert()
    {
        var id = Guid.Parse(Duplicate ? _duplicate : Guid.CreateVersion7().ToString("D"));
        var transaction = await _db.Database.BeginTransactionAsync().ConfigureAwait(false);
        try
        {
            await using var command = new NpgsqlCommand(_insert, (NpgsqlConnection)_db.Database.GetDbConnection(),
                (NpgsqlTransaction)transaction.GetDbTransaction());
            command.Parameters.Add(new NpgsqlParameter<string>("consumer", "benchmark"));
            command.Parameters.Add(new NpgsqlParameter<Guid>("id", id));
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
        }
        finally { await transaction.DisposeAsync().ConfigureAwait(false); }
    }

    [Benchmark]
    public ValueTask RelayerInbox()
    {
        _context.Id = Duplicate ? _duplicate : Guid.CreateVersion7().ToString("D");
        return _middleware.InvokeAsync(_context, static _ => default);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await using (var drop = new NpgsqlCommand($"DROP SCHEMA {PostgreSqlOutboxSchema.QuoteIdentifier(_schema)} CASCADE",
            (NpgsqlConnection)_db.Database.GetDbConnection()))
            await drop.ExecuteNonQueryAsync();
        await new AsyncServiceScope(_scope).DisposeAsync();
        await _provider.DisposeAsync();
    }

    private sealed class Context(IServiceProvider services) : MessageContext
    {
        internal string Id { get; set; } = "";
        public override string MessageId => Id;
        public override MessageType MessageType => new("benchmark", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => services;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
