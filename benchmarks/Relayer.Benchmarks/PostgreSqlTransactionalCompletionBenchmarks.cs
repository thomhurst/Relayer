using Relayer.Transports;
using BenchmarkDotNet.Attributes;
using Npgsql;
using NpgsqlTypes;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Native fenced deletion versus the store overload in the same caller-owned transaction.</summary>
[MemoryDiagnoser]
public class PostgreSqlTransactionalCompletionBenchmarks
{
    private NpgsqlDataSource _source = null!;
    private NpgsqlConnection _connection = null!;
    private PostgreSqlOutboxStore _store = null!;
    private readonly PostgreSqlOutboxOptions _options = new() { TablePrefix = "c_" + Guid.NewGuid().ToString("N")[..12] + "_" };
    private readonly OutboxClaim _claim = new(Guid.CreateVersion7(), Guid.NewGuid());
    private string _table = null!;
    private string _delete = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _source = NpgsqlDataSource.Create(Environment.GetEnvironmentVariable("RELAYER_POSTGRES_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_POSTGRES_BENCHMARK_CONNECTION to an isolated PostgreSQL database."));
        _connection = await _source.OpenConnectionAsync();
        _store = new(_source, _options);
        _table = "public." + PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "outbox");
        _delete = $"DELETE FROM {_table} WHERE id = @id AND claim_token = @token AND claim_expires_at > clock_timestamp() RETURNING true";
        await Execute(PostgreSqlOutboxSchema.CreateScript(_options));
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO {_table} (id, transport, destination, message_type, body, headers, created_at, claim_token, claim_expires_at)
            VALUES (@id, 'local', 'work', 'probe', ''::bytea, ''::bytea, clock_timestamp(), @token, 'infinity')
            """, _connection);
        AddClaims(insert);
        await insert.ExecuteNonQueryAsync();
    }

    [Benchmark(Baseline = true)]
    public async ValueTask NativeCompletion()
    {
        await using var transaction = await _connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand(_delete, _connection, transaction);
        AddClaims(command);
        if (await command.ExecuteScalarAsync() is not true) throw new InvalidOperationException("Benchmark claim was lost.");
        // Restore the same claimed row so every measured invocation performs a live deletion.
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async ValueTask StoreCompletion()
    {
        await using var transaction = await _connection.BeginTransactionAsync();
        if (!await _store.CompleteAsync(_claim, transaction, default)) throw new InvalidOperationException("Benchmark claim was lost.");
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
        await Execute($"DROP TABLE IF EXISTS {_table}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "outbox_dead")}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "inbox")}; DROP TABLE IF EXISTS public.{PostgreSqlOutboxSchema.QuoteIdentifier(_options.TablePrefix + "leases")}");
        await _connection.DisposeAsync();
        await _source.DisposeAsync();
    }
}
