using System.Buffers;
using System.Diagnostics;

namespace Relayer.Transports;

/// <summary>Reads and preserves the immutable W3C creation context in a native message.</summary>
public static class MessageCreationContext
{
    private static readonly SearchValues<char> HexCharacters = SearchValues.Create("0123456789abcdef");
    /// <summary>Reads a remote creation context. Invalid or absent traceparent returns default.</summary>
    /// <remarks>Call only when tracing or propagation needs it; UTF-8 tracestate decoding can allocate.</remarks>
    public static ActivityContext Read<TMessage, TReader>(TMessage message, HeaderNames names)
        where TReader : IMessageHeaderReader<TMessage>
    {
        ArgumentNullException.ThrowIfNull(names);
        if (!TReader.TryGetHeader(message, names.TraceParent, out var traceParent)) return default;
        if (traceParent.Text is { } text)
            return ActivityContext.TryParse(text, ReadState<TMessage, TReader>(message, names), true, out var context) ? context : default;

        Span<char> buffer = stackalloc char[55];
        if (!traceParent.TryFormat(buffer, out var written) || written != buffer.Length) return default;
        // Match ActivityContext.TryParse, including future versions, flags, and separator handling.
        if (!IsHex(buffer[..2], false) || buffer[..2].SequenceEqual("ff") ||
            !IsHex(buffer.Slice(3, 32), true) || !IsHex(buffer.Slice(36, 16), true) || !IsHex(buffer[53..], false))
            return default;
        var flags = (ActivityTraceFlags)((HexValue(buffer[53]) << 4) | HexValue(buffer[54]));
        return new(ActivityTraceId.CreateFromString(buffer.Slice(3, 32)), ActivitySpanId.CreateFromString(buffer.Slice(36, 16)),
            flags, ReadState<TMessage, TReader>(message, names), true);
    }

    private static string? ReadState<TMessage, TReader>(TMessage message, HeaderNames names)
        where TReader : IMessageHeaderReader<TMessage> =>
        TReader.TryGetHeader(message, names.TraceState, out var state) ? state.ToString() : null;

    private static bool IsHex(ReadOnlySpan<char> text, bool requireNonzero)
    {
        return !text.ContainsAnyExcept(HexCharacters) && (!requireNonzero || text.ContainsAnyExcept('0'));
    }

    private static int HexValue(char character) => character <= '9' ? character - '0' : character - 'a' + 10;

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
