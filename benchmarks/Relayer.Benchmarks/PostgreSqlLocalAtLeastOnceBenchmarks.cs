using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using NpgsqlTypes;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Native autocommitted fenced delete versus an at-least-once local consume, which runs the handler with no transaction and completes the row in its own fenced statement.</summary>
[MemoryDiagnoser]
public class PostgreSqlLocalAtLeastOnceBenchmarks
{
    private NpgsqlDataSource _source = null!;
    private NpgsqlConnection _connection = null!;
    private ServiceProvider _provider = null!;
    private PostgreSqlLocalTransport _transport = null!;
    private readonly PostgreSqlOutboxOptions _options = new() { TablePrefix = "alo_" + Guid.NewGuid().ToString("N")[..12] + "_" };
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
        await using (var create = new NpgsqlCommand(PostgreSqlOutboxSchema.CreateScript(_options), _connection))
            await create.ExecuteNonQueryAsync();
        KeyValuePair<string, WireString>[] headers = [new("ce_id", new(_claim.MessageId.ToString())), new("ce_type", new("benchmark.local;v=1")), new("ce_source", new("/benchmark")), new("ce_specversion", new("1.0"))];
        _message = new("work", "{\"Value\":1}"u8.ToArray(), headers);
        var services = new ServiceCollection().AddLogging();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.PostgreSqlLocal("local", _source, transport =>
            {
                transport.Send<PostgreSqlLocalTransportBenchmarks.LocalBenchmarkMessage>("work");
                transport.Consume("work", consumer =>
                {
                    consumer.Options.Completion = PostgreSqlLocalCompletion.AtLeastOnce;
                    consumer.Handler<PostgreSqlLocalTransportBenchmarks.LocalBenchmarkHandler>();
                });
            }, _options);
        });
        services.AddRelayerPostgreSqlDirectDispatch("local", _source, _options);
        _provider = services.BuildServiceProvider();
        foreach (var service in _provider.GetServices<IHostedService>())
            if (service is not PostgreSqlDirectDispatcher) await service.StartAsync(default);
        _transport = PostgreSqlLocalTransport.Find(_provider.GetServices<ITransport>().Single())!;
        await LocalConsume();
    }

    // Both cases insert the committed claim first, because completion deletes the row.
    [Benchmark(Baseline = true)]
    public async ValueTask NativeCompletion()
    {
        await InsertClaimedRow();
        // At-least-once completion runs on its own pooled connection, so the baseline does too.
        await using var connection = await _source.OpenConnectionAsync();
        await using var command = new NpgsqlCommand($"DELETE FROM {_table} WHERE id = @id AND claim_token = @token AND claim_expires_at > clock_timestamp() RETURNING true", connection);
        AddClaims(command);
        if (await command.ExecuteScalarAsync() is not true) throw new InvalidOperationException("Benchmark claim was lost.");
    }

    [Benchmark]
    public async ValueTask LocalConsume()
    {
        await InsertClaimedRow();
        var admission = _transport.TryEnter(_message.Destination) ?? throw new InvalidOperationException("Unexpected concurrent benchmark invocation.");
        try { await _transport.ConsumeAsync(_message, _claim, 1, null, default); }
        finally { admission.Exit(); }
    }

    private async ValueTask InsertClaimedRow()
    {
        await using var insert = new NpgsqlCommand($"INSERT INTO {_table} (id, transport, destination, message_type, body, headers, created_at, claim_token, claim_expires_at) VALUES (@id, 'local', 'work', 'benchmark.local;v=1', ''::bytea, ''::bytea, clock_timestamp(), @token, 'infinity')", _connection);
        AddClaims(insert);
        await insert.ExecuteNonQueryAsync();
    }
    private void AddClaims(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid) { TypedValue = _claim.MessageId });
        command.Parameters.Add(new NpgsqlParameter<Guid>("token", NpgsqlDbType.Uuid) { TypedValue = _claim.Token });
    }
    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _provider.DisposeAsync();
        await using (var drop = new NpgsqlCommand($"DROP TABLE IF EXISTS {_table}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "outbox_dead")}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "inbox")}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "leases")}", _connection))
            await drop.ExecuteNonQueryAsync();
        await _connection.DisposeAsync();
        await _source.DisposeAsync();
    }
}
