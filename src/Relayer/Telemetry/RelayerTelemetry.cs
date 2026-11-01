namespace Relayer;

/// <summary>Names of Relayer's OpenTelemetry sources (docs/decisions.md D15).</summary>
public static class RelayerTelemetry
{
    /// <summary>The <see cref="System.Diagnostics.ActivitySource"/> name. Add it with <c>AddSource("Relayer")</c>.</summary>
    public const string ActivitySourceName = "Relayer";

    /// <summary>The <see cref="System.Diagnostics.Metrics.Meter"/> name. Add it with <c>AddMeter("Relayer")</c>.</summary>
    public const string MeterName = "Relayer";

    /// <summary>The implemented OpenTelemetry semantic conventions version.</summary>
    public const string SemanticConventionsVersion = "1.44.0";

    /// <summary>The telemetry schema shared by Relayer's instrumentation.</summary>
    public const string SchemaUrl = "https://opentelemetry.io/schemas/" + SemanticConventionsVersion;
}
