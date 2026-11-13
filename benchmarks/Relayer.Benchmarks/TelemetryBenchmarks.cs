using Relayer.Transports;
using Relayer.Pipeline;
#pragma warning disable RLY001 // The benchmark explicitly registers its cached payload and serializer.
using System.Buffers;
using System.Diagnostics.Metrics;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Measures telemetry's incremental CPU and allocation cost with identical DI/serializer work in each case.</summary>
[MemoryDiagnoser]
public class TelemetryBenchmarks
{
    private ServiceProvider _provider = null!;
    private ConsumePipeline _baseline = null!;
    private ConsumePipeline _instrumented = null!;
    private ReceivePipeline<Incoming, Binding> _receive = null!;
    private Incoming _incoming = null!;
    private MeterListener? _listener;
    private EndpointTelemetry _endpoint = null!;
    private ConsumeMessage _message;

    [Params(false, true)]
    public bool ListenerAttached { get; set; }

    [Params(false, true)]
    public bool ErrorLoggingEnabled { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Handler>();
        services.AddLogging(logging => logging
            .SetMinimumLevel(ErrorLoggingEnabled ? LogLevel.Error : LogLevel.None)
            .AddProvider(new ScopeProvider()));
        services.AddRelayer(r => { r.Infrastructure().MessageTypes.Add<Payload>("benchmark.payload"); new ConsumerBuilder<object>(r.Services, r.Infrastructure().Handlers, "manual").Handler<Handler>(); });
        _provider = services.BuildServiceProvider();
        var instrumentation = _provider.GetRequiredService<RelayerInstrumentation>();
        _endpoint = instrumentation.CreateEndpoint(new()
        {
            System = "kafka", Destination = "orders", ConsumerGroup = "billing", Subscription = "subscription",
            ServerAddress = "localhost", ServerPort = 9092,
        });
        if (ListenerAttached)
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (ReferenceEquals(instrument.Meter.Scope, instrumentation)) listener.EnableMeasurementEvents(instrument);
                },
            };
            _listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
            _listener.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
            _listener.Start();
        }
        _baseline = Builder().Build(1);
        _instrumented = Builder().Telemetry(_endpoint).Build(1);
        _receive = new(_instrumented, new HeaderNames(), 1);
        _incoming = new();
        _message = new() { MessageId = "benchmark", MessageType = new("benchmark.payload", 1), Body = default };
    }

    private ConsumePipelineBuilder Builder() => new ConsumePipelineBuilder(_provider.GetRequiredService<IServiceScopeFactory>(),
            _provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
        .Handler(_provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>());

    [Benchmark(Baseline = true)]
    public async ValueTask ConsumerWithoutTelemetry()
    {
        using var context = _baseline.RentContext(_message);
        await _baseline.ProcessAsync(context).ConfigureAwait(false);
    }

    [Benchmark]
    public async ValueTask ConsumerWithTelemetry()
    {
        using var context = _instrumented.RentContext(_message);
        await _instrumented.ProcessAsync(context).ConfigureAwait(false);
    }

    [Benchmark]
    public ValueTask<Disposition> NativeConsumerWithTelemetry() => _receive.ProcessAsync(_incoming);

    [Benchmark]
    public void FailureMeasurement() => _endpoint.RecordDuration(TimeSpan.FromMilliseconds(1), failed: true);

    [GlobalCleanup]
    public void Cleanup()
    {
        _listener?.Dispose();
        _provider.Dispose();
    }

    // LoggerFactory supplies real external scopes; output is disabled to isolate pipeline overhead.
    private sealed class ScopeProvider : ILoggerProvider, ISupportExternalScope, ILogger
    {
        public ILogger CreateLogger(string categoryName) => this;
        public bool IsEnabled(LogLevel level) => level >= LogLevel.Error;
        public void SetScopeProvider(IExternalScopeProvider scopeProvider) { }
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => throw new NotSupportedException();
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? error, Func<TState, Exception?, string> formatter) { }
        public void Dispose() { }
    }
    private sealed class Incoming
    {
        internal byte[] Id { get; } = "benchmark"u8.ToArray();
        internal byte[] Type { get; } = "benchmark.payload;v=1"u8.ToArray();
        internal byte[] Correlation { get; } = "conversation"u8.ToArray();
    }
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => default;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch
            {
                "ce_id" => new(message.Id),
                "ce_type" => new(message.Type),
                "ce_correlationid" => new(message.Correlation),
                _ => default,
            };
            return !value.IsEmpty;
        }
    }

    public sealed class Payload;
    public sealed class Handler : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => default;
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "benchmark/cached";
        public T Deserialize<T>(ReadOnlySequence<byte> source) => (T)(object)_payload;
        public void Serialize<T>(T message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
}
