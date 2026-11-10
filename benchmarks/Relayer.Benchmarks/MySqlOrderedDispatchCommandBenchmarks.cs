using BenchmarkDotNet.Attributes;
using MySqlConnector;
using Relayer.Outbox.MySql;

namespace Relayer.Benchmarks;

/// <summary>Native object binding versus retained ordered-claim buffers, excluding database I/O.</summary>
[MemoryDiagnoser]
public class MySqlOrderedDispatchCommandBenchmarks
{
    private readonly OutboxClaim _claim = new(Guid.CreateVersion7(), Guid.NewGuid());
    private readonly MySqlStoreParameters _operation = new("UPDATE outbox SET claim_token = @token WHERE id = @id");
    private MySqlCommand _native = null!;
    [GlobalSetup]
    public void Setup()
    {
        _native = new(_operation.Command.CommandText);
        _native.Parameters.Add("id", MySqlDbType.Binary);
        _native.Parameters.Add("token", MySqlDbType.Binary);
    }
    [Benchmark(Baseline = true)]
    public MySqlCommand NativeObjectValues()
    {
        _native.Parameters["id"].Value = _claim.MessageId.ToByteArray(bigEndian: true);
        _native.Parameters["token"].Value = _claim.Token.ToByteArray(bigEndian: true);
        return _native;
    }
    [Benchmark]
    public MySqlCommand ReusedBuffers()
    {
        _operation.Claim(_claim);
        return _operation.Command;
    }
    [GlobalCleanup]
    public void Cleanup() { _operation.Command.Dispose(); _native.Dispose(); }
}
