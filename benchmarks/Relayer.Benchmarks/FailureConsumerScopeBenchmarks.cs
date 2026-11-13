using Relayer.Transports;
using Relayer.Pipeline;
#pragma warning disable RLY001 // This pipeline-only benchmark registers its serializer and message identity explicitly.
using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.InMemory;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Failure-pipeline receive with scoped handlers, which open a scope per attempt, versus singleton handlers, which open one on demand.</summary>
/// <remarks>The serializer returns a cached reference, so the allocated total is Relayer's pipeline plus any DI scope.</remarks>
[MemoryDiagnoser]
public class FailureConsumerScopeBenchmarks
{
    private ServiceProvider _provider = null!;
    private ConsumePipeline _pipeline = null!;
    private ConsumeMessage _message;

    [Params(false, true)]
    public bool ScopedHandler { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        if (ScopedHandler) services.AddScoped<Handler>();
        else services.AddSingleton<Handler>();
        services.AddRelayer(relayer =>
        {
            relayer.Infrastructure().MessageTypes.Add<Payload>("benchmark.failure-scope");

            relayer.InMemory("benchmark", memory => memory.Consume("source", consumer => consumer.Handler<Handler>()));
        });
        _provider = services.BuildServiceProvider();
        var failures = new FailurePipeline(_provider, "benchmark", "source", "consumer");
        _pipeline = new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
                _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), failures)
            .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>()).Build(contextPoolCapacity: 1);
        _message = new ConsumeMessage
        {
            MessageId = "benchmark", MessageType = new("benchmark.failure-scope", 1), Body = ReadOnlySequence<byte>.Empty,
        };
    }

    [Benchmark]
    public async ValueTask Consume()
    {
        using var context = _pipeline.RentContext(_message);
        await _pipeline.ProcessAsync(context).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public Task Cleanup() => _provider.DisposeAsync().AsTask();

    public sealed class Payload;

    public sealed class Handler : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => default;
    }

    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
}
