using System.Diagnostics;

namespace Relayer;

/// <summary>Reads and preserves the immutable W3C creation context in a native message.</summary>
public static class MessageCreationContext
{
    /// <summary>Reads a remote creation context. Invalid or absent traceparent returns default.</summary>
    /// <remarks>Call only when tracing or propagation needs it; UTF-8 tracestate decoding can allocate.</remarks>
    public static ActivityContext Read<TMessage, TReader>(TMessage message, HeaderNames names)
        where TReader : IMessageHeaderReader<TMessage>
    {
        ArgumentNullException.ThrowIfNull(names);
        if (!TReader.TryGetHeader(message, names.TraceParent, out var traceParent)) return default;
        var traceState = TReader.TryGetHeader(message, names.TraceState, out var state) ? state.ToString() : null;
        return ActivityContext.TryParse(traceParent.ToString(), traceState, true, out var context) ? context : default;
    }

    /// <summary>Writes a context only when traceparent is absent. Existing headers remain byte-for-byte unchanged, even if invalid.</summary>
    /// <returns>True when a new context was written.</returns>
    public static bool WriteIfAbsent<TMessage, TBinding>(TMessage message, HeaderNames names, ActivityContext context)
        where TBinding : IMessageHeaderReader<TMessage>, IMessageHeaderWriter<TMessage>
    {
        ArgumentNullException.ThrowIfNull(names);
        if (TBinding.TryGetHeader(message, names.TraceParent, out _) || context.TraceId == default || context.SpanId == default)
            return false;
        Span<char> buffer = stackalloc char[55];
        MessageEnvelope.WriteCreationContext<TMessage, TBinding>(message, names, context, buffer);
        return true;
    }
}
