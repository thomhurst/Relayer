using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using NpgsqlTypes;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Native savepoint/fenced completion versus the full generated local consume pipeline in the same database transaction.</summary>
[MemoryDiagnoser]
public class PostgreSqlLocalTransportBenchmarks
{
    private NpgsqlDataSource _source = null!;
    private NpgsqlConnection _connection = null!;
    private ServiceProvider _provider = null!;
    private PostgreSqlLocalTransport _transport = null!;
    private readonly PostgreSqlOutboxOptions _options = new() { TablePrefix = "local_" + Guid.NewGuid().ToString("N")[..12] + "_" };
    private readonly OutboxClaim _claim = new(Guid.CreateVersion7(), Guid.NewGuid());
    private OutgoingMessage _message;
    private string _table = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _source = NpgsqlDataSource.Create(Environment.GetEnvironmentVariable("RELAYER_POSTGRES_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_POSTGRES_BENCHMARK_CONNECTION to an isolated PostgreSQL database."));
        _connection = await _source.OpenConnectionAsync();
        _table = "public." + PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "outbox");
        await Execute(PostgreSqlOutboxSchema.CreateScript(_options));
        KeyValuePair<string, WireString>[] headers = [new("ce_id", new(_claim.MessageId.ToString())), new("ce_type", new("benchmark.local;v=1")), new("ce_source", new("/benchmark")), new("ce_specversion", new("1.0"))];
        _message = new("work", "{\"Value\":1}"u8.ToArray(), headers);
        await using (var insert = new NpgsqlCommand($"INSERT INTO {_table} (id, transport, destination, message_type, body, headers, created_at, claim_token, claim_expires_at) VALUES (@id, 'local', 'work', 'benchmark.local;v=1', ''::bytea, ''::bytea, clock_timestamp(), @token, 'infinity')", _connection))
        {
            AddClaims(insert);
            await insert.ExecuteNonQueryAsync();
        }
        var services = new ServiceCollection().AddLogging();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.PostgreSqlLocal("local", _source, transport =>
            {
                transport.Send<LocalBenchmarkMessage>("work");
                transport.Consume("work", consumer => consumer.Handler<LocalBenchmarkHandler>());
            }, _options);
        });
        services.AddRelayerPostgreSqlDirectDispatch("local", _source, _options);
        _provider = services.BuildServiceProvider();
        foreach (var service in _provider.GetServices<IHostedService>())
            if (service is not PostgreSqlDirectDispatcher) await service.StartAsync(default);
        _transport = PostgreSqlLocalTransport.Find(_provider.GetServices<ITransport>().Single())!;
        await LocalConsume();
    }

    [Benchmark(Baseline = true)]
    public async ValueTask NativeCompletion()
    {
        await using var transaction = await _connection.BeginTransactionAsync();
        await transaction.SaveAsync("relayer_local_attempt");
        await using var command = new NpgsqlCommand($"DELETE FROM {_table} WHERE id = @id AND claim_token = @token AND claim_expires_at > clock_timestamp() RETURNING true", _connection, transaction);
        AddClaims(command);
        if (await command.ExecuteScalarAsync() is not true) throw new InvalidOperationException("Benchmark claim was lost.");
        await transaction.ReleaseAsync("relayer_local_attempt");
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async ValueTask LocalConsume()
    {
        await using var transaction = await _connection.BeginTransactionAsync();
        var admission = _transport.TryEnter(_message.Destination) ?? throw new InvalidOperationException("Unexpected concurrent benchmark invocation.");
        try { await _transport.ConsumeAsync(_message, _claim, 1, transaction, default); }
        finally { admission.Exit(); }
        await transaction.RollbackAsync();
    }
    private void AddClaims(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid) { TypedValue = _claim.MessageId });
        command.Parameters.Add(new NpgsqlParameter<Guid>("token", NpgsqlDbType.Uuid) { TypedValue = _claim.Token });
    }
    private async Task Execute(string sql)
    {
        await using var command = new NpgsqlCommand(sql, _connection);
        await command.ExecuteNonQueryAsync();
    }
    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _provider.DisposeAsync();
        await Execute($"DROP TABLE IF EXISTS {_table}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "outbox_dead")}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "inbox")}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "leases")}");
        await _connection.DisposeAsync();
        await _source.DisposeAsync();
    }
    [Message("benchmark.local")]
    public sealed record LocalBenchmarkMessage(int Value);
    public sealed class LocalBenchmarkHandler : IHandler<LocalBenchmarkMessage>
    {
        public ValueTask HandleAsync(LocalBenchmarkMessage message, MessageContext context, CancellationToken cancellationToken) => default;
    }
}
