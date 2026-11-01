using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Tests;

public class BatchTracingTests
{
    [Test]
    [Arguments(1, false)]
    [Arguments(1, true)]
    [Arguments(10, false)]
    [Arguments(10, true)]
    public async Task Dropped_batch_spans_add_no_allocations_beyond_scope_and_framework_context_parsing(int size, bool links)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageSerializer, Serializer>();
        services.AddSingleton<Handler>();
        services.AddRelayer(builder =>
        {
            builder.MessageTypes.Add<Payload>("batch.tracing");
            builder.AddTransport<Options, Options, Options>("test", transport =>
                transport.Consume("source", consumer => consumer.Handler<Handler>().Batch(size, TimeSpan.FromSeconds(1))));
        });
        await using var provider = services.BuildServiceProvider();
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        using var source = new ActivitySource("Relayer");
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => ReferenceEquals(candidate, source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None
        };
        ActivitySource.AddActivityListener(listener);
        // Match ActivitySource's ambient context and exclude its root trace ID allocation.
        using var ambient = new Activity("request").Start();
        var consumer = new ConsumePipelineBuilder(scopes, provider.GetRequiredService<MessageTypeRegistry>(),
            provider.GetRequiredService<IMessageSerializer>(), static (_, error, _) => ValueTask.FromException(error))
            .BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>(),
                provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0], source);
        var message = new ConsumeMessage
        {
            MessageId = "allocation", MessageType = new("batch.tracing", 1), Body = ReadOnlySequence<byte>.Empty,
            Headers = links ? new TraceHeaders() : null
        };
        var contexts = new DefaultMessageContext[size];
        for (var iteration = 0; iteration < 1000; iteration++)
        {
            for (var i = 0; i < size; i++) contexts[i] = consumer.RentContext(message);
            await consumer.ProcessBatchAsync(contexts);
            for (var i = 0; i < size; i++) contexts[i].Dispose();
            Baseline();
        }
        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < 1000; iteration++) Baseline();
        var baseline = GC.GetAllocatedBytesForCurrentThread() - start;
        start = GC.GetAllocatedBytesForCurrentThread();
        for (var iteration = 0; iteration < 1000; iteration++)
        {
            for (var i = 0; i < size; i++) contexts[i] = consumer.RentContext(message);
            await consumer.ProcessBatchAsync(contexts);
            for (var i = 0; i < size; i++) contexts[i].Dispose();
        }
        var composed = GC.GetAllocatedBytesForCurrentThread() - start;
        await Assert.That(composed).IsEqualTo(baseline);

        void Baseline()
        {
            using var scope = scopes.CreateScope();
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
            if (links)
                for (var i = 0; i < size; i++) ActivityContext.TryParse(TraceHeaders.Parent, null, true, out _);
        }
    }

    public sealed class Options;
    [Message("batch.tracing")]
    public sealed class Payload;
    public sealed class Handler : IBatchHandler<Payload>
    {
        public ValueTask<BatchResult> HandleAsync(Batch<Payload> batch, CancellationToken cancellationToken) => new(BatchResult.Success);
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "test/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    private sealed class TraceHeaders : MessageHeaders
    {
        internal const string Parent = "00-11111111111111111111111111111111-2222222222222222-01";
        public override bool TryGetValue(string name, out WireString value)
        {
            value = name == "traceparent" ? new(Parent) : default;
            return !value.IsEmpty;
        }
    }
}
