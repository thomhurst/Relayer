using BenchmarkDotNet.Attributes;
using Npgsql;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Benchmarks;

// Isolate Relayer state retention from the native Npgsql / JSON allocations measured
// by PostgreSqlLocalTransportBenchmarks. Each invocation holds the entire wave at once.
[MemoryDiagnoser]
public class PostgreSqlLocalStatePoolBenchmarks
{
    [Params(16, 65, 193)]
    public int Concurrency { get; set; }
    private NpgsqlDataSource _source = null!;
    private PostgreSqlLocalStatePool<PostgreSqlLocalTransport.SendState> _sends = null!;
    private PostgreSqlLocalStatePool<PostgreSqlOutboxStore> _completions = null!;
    private PostgreSqlLocalTransport.SendState[] _sendWave = null!;
    private PostgreSqlOutboxStore[] _completionWave = null!;

    [GlobalSetup]
    public void Setup()
    {
        // These states hold native batch objects but do not open a connection or serialize.
        _source = NpgsqlDataSource.Create("Host=unused");
        _sends = new(16, static () => new());
        _completions = new(64, () => new(_source));
        _sendWave = new PostgreSqlLocalTransport.SendState[Concurrency];
        _completionWave = new PostgreSqlOutboxStore[Concurrency];
        SendStates();
        CompletionStates();
    }

    [Benchmark]
    public void SendStates()
    {
        for (var i = 0; i < Concurrency; i++) _sendWave[i] = _sends.Rent();
        for (var i = 0; i < Concurrency; i++) _sends.Return(_sendWave[i]);
    }

    [Benchmark]
    public void CompletionStates()
    {
        for (var i = 0; i < Concurrency; i++) _completionWave[i] = _completions.Rent();
        for (var i = 0; i < Concurrency; i++) _completions.Return(_completionWave[i]);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sends.Dispose();
        _completions.Dispose();
        _source.Dispose();
    }
}
