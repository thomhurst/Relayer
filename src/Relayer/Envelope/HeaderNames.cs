namespace Relayer;

/// <summary>CloudEvents header names compiled once for a transport's binary-mode binding.</summary>
public sealed class HeaderNames
{
    /// <summary>Creates names with a binding prefix, such as ce_ (Kafka) or cloudEvents: (AMQP).</summary>
    public HeaderNames(string prefix = "ce_")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        Id = prefix + "id";
        Type = prefix + "type";
        Source = prefix + "source";
        Time = prefix + "time";
        SpecVersion = prefix + "specversion";
        CorrelationId = prefix + "correlationid";
    }

    /// <summary>The event ID header.</summary>
    public string Id { get; }
    /// <summary>The stable event type header.</summary>
    public string Type { get; }
    /// <summary>The event source header.</summary>
    public string Source { get; }
    /// <summary>The event time header.</summary>
    public string Time { get; }
    /// <summary>The CloudEvents version header.</summary>
    public string SpecVersion { get; }
    /// <summary>The optional CloudEvents correlationid extension header.</summary>
    public string CorrelationId { get; }
    /// <summary>The W3C trace parent header.</summary>
    public string TraceParent => "traceparent";
    /// <summary>The W3C trace state header.</summary>
    public string TraceState => "tracestate";
}
