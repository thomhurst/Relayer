using System.Diagnostics;
using System.Globalization;

namespace Relayer;

/// <summary>A route's envelope writer, configured once at startup and reusable across messages.</summary>
public sealed class MessageEnvelope
{
    private readonly string _wireType;
    private readonly string _source;
    private readonly HeaderNames _names;
    private readonly MessageEnvelopeMode _mode;

    /// <summary>Creates a writer. Raw mode writes no envelope or trace headers.</summary>
    public MessageEnvelope(MessageType messageType, string source, HeaderNames names,
        MessageEnvelopeMode mode = MessageEnvelopeMode.CloudEvents)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType.Name);
        ArgumentOutOfRangeException.ThrowIfLessThan(messageType.Version, 1);
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        if (!Uri.TryCreate(source, UriKind.RelativeOrAbsolute, out _))
        {
            throw new ArgumentException("The CloudEvents source must be a URI reference.", nameof(source));
        }

        ArgumentNullException.ThrowIfNull(names);
        if (mode is not (MessageEnvelopeMode.CloudEvents or MessageEnvelopeMode.Raw))
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        _wireType = messageType.ToString();
        _source = source;
        _names = names;
        _mode = mode;
    }

    /// <summary>Writes binary-mode attributes without allocating intermediate strings.</summary>
    /// <remarks>Native header storage may allocate. The metadata's creation context is used, never Activity.Current.</remarks>
    public void Write<TOutgoing, TWriter>(TOutgoing message, in MessageEnvelopeMetadata metadata)
        where TWriter : IMessageHeaderWriter<TOutgoing>
    {
        if (_mode == MessageEnvelopeMode.Raw)
        {
            return;
        }

        if (metadata.MessageId == Guid.Empty)
        {
            throw new ArgumentException("CloudEvents requires a nonempty message ID.", nameof(metadata));
        }

        Span<char> buffer = stackalloc char[64];
        metadata.MessageId.TryFormat(buffer, out var length, "D");
        TWriter.SetHeader(message, _names.Id, buffer[..length]);
        TWriter.SetHeader(message, _names.Type, _wireType);
        TWriter.SetHeader(message, _names.Source, _source);
        metadata.Time.UtcDateTime.TryFormat(buffer, out length, "O", CultureInfo.InvariantCulture);
        TWriter.SetHeader(message, _names.Time, buffer[..length]);
        TWriter.SetHeader(message, _names.SpecVersion, "1.0");

        var context = metadata.CreationContext;
        if (context.TraceId == default || context.SpanId == default)
        {
            TWriter.RemoveHeader(message, _names.TraceParent);
            TWriter.RemoveHeader(message, _names.TraceState);
            return;
        }

        // W3C version 00: 2 version + 32 trace ID + 16 span ID + 2 flags + 3 separators.
        buffer[0] = '0';
        buffer[1] = '0';
        buffer[2] = '-';
        Span<byte> traceBytes = stackalloc byte[16];
        context.TraceId.CopyTo(traceBytes);
        Convert.TryToHexStringLower(traceBytes, buffer[3..35], out _);
        buffer[35] = '-';
        context.SpanId.CopyTo(traceBytes[..8]);
        Convert.TryToHexStringLower(traceBytes[..8], buffer[36..52], out _);
        buffer[52] = '-';
        buffer[53] = '0';
        buffer[54] = (context.TraceFlags & ActivityTraceFlags.Recorded) != 0 ? '1' : '0';
        TWriter.SetHeader(message, _names.TraceParent, buffer[..55]);
        if (!string.IsNullOrEmpty(context.TraceState))
        {
            TWriter.SetHeader(message, _names.TraceState, context.TraceState);
        }
        else
        {
            TWriter.RemoveHeader(message, _names.TraceState);
        }
    }
}
