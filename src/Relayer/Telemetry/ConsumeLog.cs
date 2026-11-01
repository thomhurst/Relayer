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
    private DefaultMessageContext? _context;
    private string? _id;
    private string? _type;
    private string? _correlation;
    public int Count => 3;
    public KeyValuePair<string, object?> this[int index] => index switch
    {
        0 => new("MessageId", _id ??= _context?.GetLogMessageId()),
        1 => new("MessageType", _type ??= _context?.GetLogMessageType()),
        2 => new("CorrelationId", _correlation ??= GetCorrelationId()),
        _ => throw new ArgumentOutOfRangeException(nameof(index)),
    };

    internal void Initialize(DefaultMessageContext context) => _context = context;

    private string? GetCorrelationId() =>
        _context is { } context && context.Headers.TryGetValue(context.EnvelopeNames.CorrelationId, out var value)
            ? value.ToString() : null;

    internal void Clear()
    {
        _context = null;
        (_id, _type, _correlation) = (null, null, null);
    }
    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        for (var i = 0; i < Count; i++) yield return this[i];
    }
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    public override string ToString() => $"MessageId={this[0].Value} MessageType={this[1].Value} CorrelationId={this[2].Value}";
}
