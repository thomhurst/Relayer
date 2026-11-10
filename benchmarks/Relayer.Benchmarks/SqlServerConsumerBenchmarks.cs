#pragma warning disable RLY001 // This benchmark uses an explicitly registered cached serializer.
using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.SqlServer;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Native inbox transaction versus scoped consumer boundary, including SqlClient I/O and allocations.</summary>
[MemoryDiagnoser]
public class SqlServerConsumerBenchmarks
{
    private readonly SqlServerOutboxOptions _options = new() { TablePrefix = "consumer_" + Guid.NewGuid().ToString("N") + "_" };
    private string _connection = null!;
    private string _table = null!;
    private string _insert = null!;
    private string _id = null!;
    private ServiceProvider _provider = null!;
    private ConsumePipeline _pipeline = null!;

    [Params(false, true)]
    public bool Duplicate { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _connection = Environment.GetEnvironmentVariable("RELAYER_SQLSERVER_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_SQLSERVER_BENCHMARK_CONNECTION to an isolated SQL Server database.");
        _table = $"[dbo].[{_options.TablePrefix}inbox]";
        _insert = $"INSERT INTO {_table} (consumer, message_id) SELECT @consumer, @id WHERE NOT EXISTS (SELECT 1 FROM {_table} WITH (UPDLOCK, HOLDLOCK) WHERE consumer = @consumer AND message_id = @id); SELECT @@ROWCOUNT";
        _id = Guid.CreateVersion7().ToString("D");
        var services = new ServiceCollection();
        services.AddRelayerSqlServerOutbox(_options);
        services.AddRelayer(r => { r.MessageTypes.Add<Probe>("benchmark.sql-consumer"); r.Handler<Handler>(); });
        _provider = services.BuildServiceProvider();
        _pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
                _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
            .SqlServer("benchmark", _connection, inbox: true)
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>()).Build(contextPoolCapacity: 1);
        await Execute(SqlServerOutboxSchema.CreateScript(_options));
        if (Duplicate) await NativeInbox();
    }

    // IterationSetup limits invocation count to one, keeping fresh-key measurements bounded.
    [IterationSetup]
    public void Reset()
    {
        if (Duplicate) return;
        using var connection = new SqlConnection(_connection);
        connection.Open();
        using var command = new SqlCommand($"DELETE FROM {_table}", connection);
        command.ExecuteNonQuery();
    }

    [Benchmark(Baseline = true)]
    public async Task NativeInbox()
    {
        await using var connection = new SqlConnection(_connection);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await using var command = new SqlCommand(_insert, connection, transaction);
        command.Parameters.Add(new SqlParameter("@consumer", System.Data.SqlDbType.NVarChar, 256) { Value = "benchmark" });
        command.Parameters.Add(new SqlParameter("@id", System.Data.SqlDbType.UniqueIdentifier) { Value = Guid.Parse(_id) });
        _ = (int)(await command.ExecuteScalarAsync())!;
        await transaction.CommitAsync();
    }

    [Benchmark]
    public async ValueTask ConsumerInbox()
    {
        using var context = _pipeline.RentContext(new ConsumeMessage
        {
            MessageId = _id, MessageType = new("benchmark.sql-consumer", 1), Body = ReadOnlySequence<byte>.Empty
        });
        await _pipeline.ProcessAsync(context);
    }

    private async Task Execute(string sql)
    {
        await using var connection = new SqlConnection(_connection);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _provider.DisposeAsync();
        await Execute($"DROP TABLE {_table}, [dbo].[{_options.TablePrefix}outbox]");
    }
    public sealed class Probe;
    public sealed class Handler : IHandler<Probe>
    {
        public ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Probe _probe = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_probe;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
}
