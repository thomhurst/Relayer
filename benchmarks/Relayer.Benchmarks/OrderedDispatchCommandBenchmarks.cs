using Relayer.Transports;
using BenchmarkDotNet.Attributes;
using Npgsql;
using NpgsqlTypes;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

/// <summary>Compares per-row command construction with retained typed parameter binding. No database I/O.</summary>
[MemoryDiagnoser]
public class OrderedDispatchCommandBenchmarks
{
    private readonly OutboxClaim _claim = new(Guid.CreateVersion7(), Guid.NewGuid());
    private NpgsqlCommand _command = null!;
    private readonly string _sql = "UPDATE outbox SET claim_token = @token, claim_expires_at = clock_timestamp() + @duration WHERE id = @id";

    [GlobalSetup]
    public void Setup() => _command = CreateCommand();

    [GlobalCleanup]
    public void Cleanup() => _command.Dispose();

    [Benchmark(Baseline = true)]
    public Guid ConstructAndBind()
    {
        using var command = CreateCommand();
        PostgreSqlOrderedLane.SetClaim(command, _claim);
        return ((NpgsqlParameter<Guid>)command.Parameters[0]).TypedValue;
    }

    [Benchmark]
    public NpgsqlCommand Rebind() => PostgreSqlOrderedLane.SetClaim(_command, _claim);

    private NpgsqlCommand CreateCommand()
    {
        var command = new NpgsqlCommand(_sql);
        command.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid));
        command.Parameters.Add(new NpgsqlParameter<Guid>("token", NpgsqlDbType.Uuid));
        command.Parameters.Add(new NpgsqlParameter<TimeSpan>("duration", NpgsqlDbType.Interval) { TypedValue = TimeSpan.FromSeconds(30) });
        return command;
    }
}
