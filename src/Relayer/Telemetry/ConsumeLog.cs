using Relayer.Pipeline;
using System.Collections.ObjectModel;
using Microsoft.Extensions.Logging;

namespace Relayer;

internal static partial class ConsumeLog
{
    [LoggerMessage(1, LogLevel.Error, "Message processing failed. TraceId={TraceId} SpanId={SpanId}.")]
    internal static partial void Failed(ILogger logger, Exception exception, string? traceId, string? spanId);

    // Snapshot only for an emitted failure. Providers can retain it after the context returns to its pool.
    internal static IReadOnlyList<KeyValuePair<string, object?>> CreateScope(DefaultMessageContext context)
    {
        var correlation = context.Headers.TryGetValue(context.EnvelopeNames.CorrelationId, out var value)
            ? value.ToString() : null;
        return new ConsumeLogScope(context.GetLogMessageId(), context.GetLogMessageType(), correlation);
    }
}

internal sealed class ConsumeLogScope(string? messageId, string? messageType, string? correlationId)
    : ReadOnlyCollection<KeyValuePair<string, object?>>([
        new("MessageId", messageId), new("MessageType", messageType), new("CorrelationId", correlationId),
    ])
{
    public override string ToString() => $"MessageId={this[0].Value} MessageType={this[1].Value} CorrelationId={this[2].Value}";
}
