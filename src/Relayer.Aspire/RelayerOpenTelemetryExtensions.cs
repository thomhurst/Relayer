using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Relayer.Aspire;

/// <summary>Connects existing Relayer instrumentation to the application's OpenTelemetry providers.</summary>
public static class RelayerOpenTelemetryExtensions
{
    /// <summary>Subscribes to Relayer's ActivitySource and Meter. Configure exporters and sampling in ServiceDefaults or application code.</summary>
    public static OpenTelemetryBuilder AddRelayerInstrumentation(this OpenTelemetryBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTracing(static tracing => tracing.AddSource(RelayerTelemetry.ActivitySourceName))
            .WithMetrics(static metrics => metrics.AddMeter(RelayerTelemetry.MeterName));
    }
}
