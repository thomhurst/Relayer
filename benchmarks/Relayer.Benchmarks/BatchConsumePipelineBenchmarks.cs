using Relayer.Transports;
using Relayer.Pipeline;
using System.Buffers;
using System.Diagnostics;
using System.Text;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Batch composition versus the same scoped sender, serializer and generated handler dispatch.</summary>
/// <remarks>Results are per batch. DI scope allocations are shared by both paths; payload instances are cached.</remarks>
[MemoryDiagnoser]
public class BatchConsumePipelineBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScopeFactory _scopes = null!;
    private ConsumePipeline _pipeline = null!;
    private Handler _handler = null!;
    private BatchHandlerDescriptor<Payload> _descriptor = null!;
    private readonly Serializer _serializer = new();
    private readonly DirectContext _directContext = new();
    private Batch<Payload> _directBatch = null!;
    private ConsumeMessage _message;
    private DefaultMessageContext[] _contexts = null!;
    private ActivitySource _source = null!;
    private ActivityListener _listener = null!;
    private Activity _ambient = null!;
    private ActivityLink[] _links = null!;
    private KeyValuePair<string, object?>[] _tags = null!;
    private readonly HeaderNames _headerNames = new();

    [Params(1, 10, 100)]
    public int BatchSize { get; set; }

    [Params(TracingMode.None, TracingMode.Text, TracingMode.Utf8)]
    public TracingMode Tracing { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IMessageSerializer>(_serializer);
        services.AddSingleton<Handler>();
        services.AddRelayer(builder =>
        {
            builder.Infrastructure().MessageTypes.Add<Payload>("benchmark.batch");
            builder.AddTransport<Options, Options>("benchmark", transport =>
                transport.Consume("source", consumer => consumer.Handler<Handler>().Batch(BatchSize, TimeSpan.FromMilliseconds(100))));
        });
        _provider = services.BuildServiceProvider();
        _scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        _handler = _provider.GetRequiredService<Handler>();
        _descriptor = _provider.GetRequiredService<HandlerRegistry>().GetBatchHandler<Payload, Handler>();
        var definition = _provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0];
        _source = new ActivitySource("Relayer");
        _listener = new ActivityListener
        {
            ShouldListenTo = source => Tracing != TracingMode.None && ReferenceEquals(source, _source),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None
        };
        ActivitySource.AddActivityListener(_listener);
        _ambient = new Activity("request").Start();
        _links = new ActivityLink[BatchSize];
        _tags =
        [
            new("messaging.operation.type", "process"), new("messaging.operation.name", "process"),
            new("messaging.destination.name", "source"), new("messaging.batch.message_count", BatchSize)
        ];
        _pipeline = new ConsumePipelineBuilder(_scopes, _provider.GetRequiredService<MessageTypeRegistry>(), _serializer,
            static (_, _, _) => default).BuildBatch(_descriptor, definition, _source);
        _message = new() { MessageId = "benchmark", MessageType = new("benchmark.batch", 1), Body = ReadOnlySequence<byte>.Empty,
            Headers = Tracing != TracingMode.None ? new TraceHeaders(Tracing == TracingMode.Utf8) : null };
        _contexts = new DefaultMessageContext[BatchSize];
        var items = new BatchItem<Payload>[BatchSize];
        for (var i = 0; i < items.Length; i++) items[i] = new(_serializer.Deserialize<Payload>(_message.Body), _directContext);
        _directBatch = new(items);
    }

    [Benchmark(Baseline = true)]
    public async ValueTask DirectScopedBatch()
    {
        _handler.FailLastItem = false;
        using var activity = StartDirectActivity();
        var scope = new AsyncServiceScope(_scopes.CreateScope());
        try
        {
            _directContext.Provider = scope.ServiceProvider;
            _ = scope.ServiceProvider.GetRequiredService<ISender>();
            for (var i = 0; i < BatchSize; i++) _ = _serializer.Deserialize<Payload>(_message.Body);
            await _descriptor.DispatchAsync(_descriptor.Resolve(scope.ServiceProvider), _directBatch, default).ConfigureAwait(false);
        }
        finally { await scope.DisposeAsync().ConfigureAwait(false); }
    }

    private Activity? StartDirectActivity()
    {
        if (Tracing == TracingMode.None) return null;
        for (var i = 0; i < BatchSize; i++)
        {
            var context = MessageCreationContext.Read<MessageHeaders, CreationHeaderReader>(_message.Headers!, _headerNames);
            _links[i] = new(context);
        }
        return _source.StartActivity("process source", ActivityKind.Consumer, Activity.Current?.Context ?? default, _tags, _links);
    }

    [Benchmark]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    public async ValueTask ComposedBatch(bool manualCompletion, bool failLastItem)
    {
        _handler.ManualCompletion = manualCompletion;
        _handler.FailLastItem = failLastItem;
        for (var i = 0; i < BatchSize; i++) _contexts[i] = _pipeline.RentContext(_message);
        try { await _pipeline.ProcessBatchAsync(_contexts).ConfigureAwait(false); }
        finally
        {
            for (var i = 0; i < BatchSize; i++) { _contexts[i].Dispose(); _contexts[i] = null!; }
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ambient.Dispose();
        _listener.Dispose();
        _source.Dispose();
        _provider.Dispose();
    }
    public sealed class Options;
    [Message("benchmark.batch")]
    public sealed class Payload { public int Value => 42; }
    public sealed class Handler : IBatchHandler<Payload>
    {
        public int Total { get; private set; }
        public bool ManualCompletion { get; set; }
        public bool FailLastItem { get; set; }
        private readonly Exception _error = new InvalidOperationException("benchmark failure");
        public ValueTask<BatchResult> HandleAsync(Batch<Payload> batch, CancellationToken cancellationToken)
        {
            var total = 0;
            for (var i = 0; i < batch.Count; i++)
            {
                total += batch.Items[i].Message.Value;
                if (ManualCompletion) batch.Items[i].Context.Features.GetRequired<ISettlementFeature>().Complete();
            }
            if (FailLastItem) batch.Items[batch.Count - 1].Fail(_error);
            Total = total;
            return new(BatchResult.Success);
        }
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    public enum TracingMode { None, Text, Utf8 }

    private readonly struct CreationHeaderReader : IMessageHeaderReader<MessageHeaders>
    {
        public static bool TryGetHeader(MessageHeaders message, string name, out WireString value) =>
            message.TryGetValue(name, out value);
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
    private sealed class DirectContext : MessageContext
    {
        public IServiceProvider Provider { get; set; } = null!;
        public override string MessageId => "benchmark";
        public override MessageType MessageType => new("benchmark.batch", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => Provider;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
