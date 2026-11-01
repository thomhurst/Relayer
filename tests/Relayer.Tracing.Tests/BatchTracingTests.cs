using System.Buffers;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Tests;

public class BatchTracingTests
{
    [Test]
    [Arguments(1, false, false, true)]
    [Arguments(1, true, false, true)]
    [Arguments(1, true, true, true)]
    [Arguments(10, false, false, true)]
    [Arguments(10, true, false, true)]
    [Arguments(10, true, true, true)]
    [Arguments(1, true, false, false)]
    [Arguments(10, true, true, false)]
    public async Task Batch_spans_add_no_allocations_beyond_scope_and_framework_context_parsing(int size, bool links, bool utf8, bool listenerAttached)
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
        using var tracing = new MessageTracing(Microsoft.Extensions.Options.Options.Create(new TracingOptions()), []);
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => listenerAttached && ReferenceEquals(candidate, tracing.Source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None
        };
        ActivitySource.AddActivityListener(listener);
        // Match ActivitySource's ambient context and exclude its root trace ID allocation.
        using var ambient = new Activity("request").Start();
        var consumer = new ConsumePipelineBuilder(scopes, provider.GetRequiredService<MessageTypeRegistry>(),
            provider.GetRequiredService<IMessageSerializer>(), static (_, error, _) => ValueTask.FromException(error))
            .Tracing(tracing, new() { System = "relayer.inmemory", Destination = "source" })
            .BuildBatch(provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>(),
                provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0]);
        var message = new ConsumeMessage
        {
            MessageId = "allocation", MessageType = new("batch.tracing", 1), Body = ReadOnlySequence<byte>.Empty,
            Headers = links ? new TraceHeaders(utf8) : null
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
            if (links && listenerAttached)
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
    private sealed class TraceHeaders(bool utf8) : MessageHeaders
    {
        internal const string Parent = "00-11111111111111111111111111111111-2222222222222222-01";
        private readonly WireString _parent = utf8 ? new(Encoding.UTF8.GetBytes(Parent)) : new(Parent);
        public override bool TryGetValue(string name, out WireString value)
        {
            value = name == "traceparent" ? _parent : default;
            return !value.IsEmpty;
        }
    }
}
