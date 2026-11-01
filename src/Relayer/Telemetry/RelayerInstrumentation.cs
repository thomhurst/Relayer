using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Relayer;

/// <summary>Owns the application's Relayer meter. DI disposes it with the service provider.</summary>
/// <remarks>Use the registered singleton across endpoints. Standalone callers must dispose their instance.</remarks>
public sealed class RelayerInstrumentation : IDisposable
{
    private readonly Meter _meter;
    private readonly ILogger _logger;
    internal Histogram<double> OperationDuration { get; }
    internal Histogram<double> ProcessDuration { get; }
    internal Counter<long> Sent { get; }
    internal Counter<long> Consumed { get; }
    internal Histogram<double> CriticalTime { get; }
    internal Counter<long> Retries { get; }
    internal Counter<long> DeadLettered { get; }
    internal UpDownCounter<long> OutboxPending { get; }
    internal Histogram<double> DispatchLag { get; }
    internal Counter<long> Duplicates { get; }
    internal Gauge<long> ConsumerLag { get; }
    internal Gauge<int> CircuitState { get; }

    /// <summary>Creates instrumentation with an optional application logger factory.</summary>
    public RelayerInstrumentation(ILoggerFactory? loggerFactory = null)
    {
        _logger = loggerFactory?.CreateLogger<ConsumePipeline>() ?? NullLogger<ConsumePipeline>.Instance;
        _meter = new Meter(new MeterOptions(RelayerTelemetry.MeterName)
        {
            TelemetrySchemaUrl = RelayerTelemetry.SchemaUrl,
            Scope = this,
        });
        // https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/messaging/messaging-metrics.md
        var advice = new InstrumentAdvice<double>
        {
            HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10],
        };
        OperationDuration = _meter.CreateHistogram<double>("messaging.client.operation.duration", "s", advice: advice);
        ProcessDuration = _meter.CreateHistogram<double>("messaging.process.duration", "s", advice: advice);
        Sent = _meter.CreateCounter<long>("messaging.client.sent.messages", "{message}");
        Consumed = _meter.CreateCounter<long>("messaging.client.consumed.messages", "{message}");
        CriticalTime = _meter.CreateHistogram<double>("relayer.critical_time", "s");
        Retries = _meter.CreateCounter<long>("relayer.retries", "{retry}");
        DeadLettered = _meter.CreateCounter<long>("relayer.dead_lettered", "{message}");
        OutboxPending = _meter.CreateUpDownCounter<long>("relayer.outbox.pending", "{message}");
        DispatchLag = _meter.CreateHistogram<double>("relayer.outbox.dispatch_lag", "s");
        Duplicates = _meter.CreateCounter<long>("relayer.inbox.duplicates", "{message}");
        ConsumerLag = _meter.CreateGauge<long>("relayer.consumer.lag", "{message}");
        CircuitState = _meter.CreateGauge<int>("relayer.circuit.state");
    }

    /// <summary>Precomputes one operation's tags. Reuse the returned recorder concurrently across deliveries.</summary>
    public EndpointTelemetry CreateEndpoint(EndpointTelemetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.System);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.OperationName);
        if (options.ServerPort is { } port) ArgumentOutOfRangeException.ThrowIfNegativeOrZero(port);
        if (options.ServerPort > 65535) throw new ArgumentOutOfRangeException(nameof(options), "ServerPort must not exceed 65535.");
        return new(this, options, _logger);
    }

    /// <summary>Stops publishing every instrument owned by this instance.</summary>
    public void Dispose() => _meter.Dispose();
}
