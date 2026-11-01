using System.Collections;
using Microsoft.Extensions.Logging;

namespace Relayer;

internal static partial class ConsumeLog
{
    [LoggerMessage(1, LogLevel.Error, "Message processing failed. TraceId={TraceId} SpanId={SpanId}.")]
    internal static partial void Failed(ILogger logger, Exception exception, string? traceId, string? spanId);
}

// Each pooled context owns its scope state; it is never shared across concurrent deliveries.
internal sealed class ConsumeLogScope : IReadOnlyList<KeyValuePair<string, object?>>
{
    private string? _id;
    private string? _type;
    private string? _correlation;
    public int Count => 3;
    public KeyValuePair<string, object?> this[int index] => index switch
    {
        0 => new("MessageId", _id),
        1 => new("MessageType", _type),
        2 => new("CorrelationId", _correlation),
        _ => throw new ArgumentOutOfRangeException(nameof(index)),
    };

    internal void Initialize(DefaultMessageContext context)
    {
        context.GetLogIdentity(out _id, out _type);
        _correlation = context.Headers.TryGetValue("correlation-id", out var value) ? value.ToString() : null;
    }

    internal void Clear() => (_id, _type, _correlation) = (null, null, null);
    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        for (var i = 0; i < Count; i++) yield return this[i];
    }
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public override string ToString() => $"MessageId={_id} MessageType={_type} CorrelationId={_correlation}";
}
