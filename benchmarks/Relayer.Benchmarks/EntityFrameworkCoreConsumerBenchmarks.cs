using Relayer.Transports;
using Relayer.Pipeline;
#pragma warning disable RLY001 // The benchmark explicitly registers its cached serializer and wire identity.
using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Measures the consumer transaction with and without native strategy callbacks and fresh attempt scopes.</summary>
/// <remarks>Requires RELAYER_BENCHMARK_POSTGRES; no tables or durable rows are created.</remarks>
[MemoryDiagnoser]
public class EntityFrameworkCoreConsumerBenchmarks
{
    private ServiceProvider _provider = null!;
    private ConsumePipeline _pipeline = null!;
    private ConsumeMessage _message;

    [Params(false, true)]
    public bool NativeRetries { get; set; }

    [Params(false, true)]
    public bool ExplicitIsolation { get; set; }

    [Params(false, true)]
    public bool Tenancy { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var connection = Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_POSTGRES")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL benchmark database.");
        var services = new ServiceCollection();
        if (Tenancy) services.AddRelayerTenancy();
        services.AddDbContext<Context>(o => o.UseNpgsql(connection, n =>
        {
            if (NativeRetries) n.EnableRetryOnFailure();
        }));
        services.AddRelayer(r =>
        {
            r.Infrastructure().MessageTypes.Add<Probe>("benchmark.ef-consumer");
            new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>();
        });
        _provider = services.BuildServiceProvider();
        _pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
                _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(),
                static (_, error, _) => ValueTask.FromException(error))
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Probe, Handler>())
            .EntityFrameworkCore<Context>("benchmark", isolationLevel: ExplicitIsolation ? System.Data.IsolationLevel.Serializable : null).Build(contextPoolCapacity: 1);
        _message = new ConsumeMessage { MessageId = "benchmark", MessageType = new("benchmark.ef-consumer", 1), Body = ReadOnlySequence<byte>.Empty };
    }

    [Benchmark]
    public async ValueTask Consume()
    {
        using var context = _pipeline.RentContext(_message);
        await _pipeline.ProcessAsync(context).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public ValueTask Cleanup() => _provider.DisposeAsync();

    public sealed class Context(DbContextOptions<Context> options) : DbContext(options);
    public sealed class Probe;
    public sealed class Handler(Context database) : IHandler<Probe>
    {
        public ValueTask HandleAsync(Probe message, MessageContext context, CancellationToken cancellationToken)
            => new(database.Database.ExecuteSqlRawAsync("SELECT 1", cancellationToken));
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Probe _probe = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_probe;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
}
