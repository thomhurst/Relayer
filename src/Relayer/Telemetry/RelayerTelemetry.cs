namespace Relayer;

/// <summary>Names of Relayer's OpenTelemetry sources (docs/decisions.md D15).</summary>
public static class RelayerTelemetry
{
    /// <summary>The conventions version implemented by Relayer (D25).</summary>
    public const string SemanticConventionsVersion = "1.37.0";

    /// <summary>The immutable schema for the implemented conventions.</summary>
    public const string SchemaUrl = "https://opentelemetry.io/schemas/" + SemanticConventionsVersion;

    /// <summary>The <see cref="System.Diagnostics.ActivitySource"/> name. Add it with <c>AddSource("Relayer")</c>.</summary>
    public const string ActivitySourceName = "Relayer";

    /// <summary>The <see cref="System.Diagnostics.Metrics.Meter"/> name. Add it with <c>AddMeter("Relayer")</c>.</summary>
    public const string MeterName = "Relayer";
}
