#pragma warning disable RLY001 // The benchmark explicitly registers its cached payload and serializer.
using System.Buffers;
using System.Diagnostics.Metrics;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Benchmarks;

/// <summary>Measures telemetry's incremental CPU and allocation cost with identical DI/serializer work in each case.</summary>
[MemoryDiagnoser]
public class TelemetryBenchmarks
{
    private ServiceProvider _provider = null!;
    private ConsumePipeline _baseline = null!;
    private ConsumePipeline _instrumented = null!;
    private MeterListener? _listener;
    private EndpointTelemetry _endpoint = null!;
    private ConsumeMessage _message;

    [Params(false, true)]
    public bool ListenerAttached { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Handler>();
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("benchmark.payload"); r.Handler<Handler>(); });
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
    public void FailureMeasurement() => _endpoint.RecordDuration(TimeSpan.FromMilliseconds(1), failed: true);

    [GlobalCleanup]
    public void Cleanup()
    {
        _listener?.Dispose();
        _provider.Dispose();
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
