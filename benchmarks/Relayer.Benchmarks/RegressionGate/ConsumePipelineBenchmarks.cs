#pragma warning disable RLY001 // This pipeline-only benchmark registers its serializer and message identity explicitly.
using System.Buffers;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Full consume composition versus identical fresh-scope DI, deserialization and typed dispatch.</summary>
/// <remarks>
/// ScopeOnly and ScopeAndHandlerResolution attribute DI costs before dispatch. DirectScopedDispatch and
/// ComposedConsumer include those costs; their delta measures composition, not compliance with the raw-client budget.
/// The serializer returns a cached reference so serialization does not obscure allocations.
/// </remarks>
[MemoryDiagnoser]
[BenchmarkCategory("RegressionGate")]
public class ConsumePipelineBenchmarks
{
    private ServiceProvider _provider = null!;
    private IServiceScopeFactory _scopes = null!;
    private ConsumePipeline _pipeline = null!;
    private HandlerDescriptor<Payload> _descriptor = null!;
    private readonly Serializer _serializer = new();
    private readonly DirectContext _directContext = new();
    private ConsumeMessage _message;

    [Params(false, true)]
    public bool ScopedHandler { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        if (ScopedHandler) services.AddScoped<Handler>();
        else services.AddSingleton<Handler>();
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("benchmark.payload"); r.AddTransport<object, object, object>("manual-1", transport => transport.Consume("source", consumer => consumer.Handler<Handler>())); });
        _provider = services.BuildServiceProvider();
        _scopes = _provider.GetRequiredService<IServiceScopeFactory>();
        var handlers = _provider.GetRequiredService<HandlerRegistry>();
        _descriptor = handlers.GetHandler<Payload, Handler>();
        _pipeline = new ConsumePipelineBuilder(_scopes, _provider.GetRequiredService<MessageTypeRegistry>(),
                _serializer, static (_, error, _) => ValueTask.FromException(error))
            .Handler(_descriptor).Use(new PassThrough()).Build(contextPoolCapacity: 1);
        _message = new ConsumeMessage
        {
            MessageId = "benchmark", MessageType = new("benchmark.payload", 1), Body = ReadOnlySequence<byte>.Empty,
        };
    }

    [Benchmark]
    public ValueTask ScopeOnly()
    {
        var scope = new AsyncServiceScope(_scopes.CreateScope());
        return scope.DisposeAsync();
    }

    [Benchmark]
    public async ValueTask<IHandlerMarker> ScopeAndHandlerResolution()
    {
        var scope = new AsyncServiceScope(_scopes.CreateScope());
        try
        {
            return _descriptor.Resolve(scope.ServiceProvider);
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Benchmark(Baseline = true)]
    public async ValueTask DirectScopedDispatch()
    {
        var scope = new AsyncServiceScope(_scopes.CreateScope());
        try
        {
            _directContext.Provider = scope.ServiceProvider;
            var message = _serializer.Deserialize<Payload>(_message.Body);
            var handler = _descriptor.Resolve(scope.ServiceProvider);
            await _descriptor.DispatchAsync(handler, message, _directContext, default).ConfigureAwait(false);
        }
        finally
        {
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async ValueTask ComposedConsumer()
    {
        using var context = _pipeline.RentContext(_message);
        await _pipeline.ProcessAsync(context).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    public sealed class Payload
    {
        public int Value { get; init; } = 42;
    }

    public sealed class Handler : IHandler<Payload>
    {
        public int LastValue { get; private set; }
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            LastValue = message.Value;
            return default;
        }
    }

    private sealed class PassThrough : IConsumeMiddleware
    {
        public ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next) => next(context);
    }

    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }

    private sealed class DirectContext : MessageContext
    {
        public IServiceProvider Provider { get; set; } = null!;
        public override string MessageId => "benchmark";
        public override MessageType MessageType => new("benchmark.payload", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => Provider;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
