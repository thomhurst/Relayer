using BenchmarkDotNet.Attributes;
using MySqlConnector;
using Relayer.Outbox.MySql;

namespace Relayer.Benchmarks;

/// <summary>Compares native object-valued parameter assignment with reusable wire buffers, without database I/O.</summary>
[MemoryDiagnoser]
public class MySqlStoreParameterBenchmarks
{
    private MySqlStoreParameters _buffered = null!;
    private MySqlCommand _native = null!;
    private readonly OutboxClaim _claim = new(Guid.Parse("01900000-0000-7000-8000-000000000001"),
        Guid.Parse("01900000-0000-7000-8000-000000000002"));
    private readonly DateTimeOffset _now = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private DateTimeOffset _expires;

    [GlobalSetup]
    public void Setup()
    {
        _expires = _now.AddMinutes(1);
        _buffered = new("SELECT @id, @token, @now, @expires, @lane, @after, @limit");
        _native = new() { CommandText = _buffered.Command.CommandText };
        foreach (var name in new[] { "id", "token", "now", "expires", "lane", "after", "limit" })
            _native.Parameters.AddWithValue(name, DBNull.Value);
        ReusableBuffers();
    }

    [Benchmark(Baseline = true)]
    public MySqlCommand NativeObjectValues()
    {
        _native.Parameters["id"].Value = _claim.MessageId;
        _native.Parameters["token"].Value = _claim.Token;
        _native.Parameters["now"].Value = _now;
        _native.Parameters["expires"].Value = _expires;
        _native.Parameters["lane"].Value = 2;
        _native.Parameters["after"].Value = _claim.MessageId;
        _native.Parameters["limit"].Value = 64;
        return _native;
    }

    [Benchmark]
    public MySqlCommand ReusableBuffers()
    {
        _buffered.Now(_now);
        _buffered.Expires(_expires);
        _buffered.Claim(_claim);
        _buffered.Read(2, _claim.MessageId, 64);
        return _buffered.Command;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _buffered.Command.Dispose();
        _native.Dispose();
    }
}
