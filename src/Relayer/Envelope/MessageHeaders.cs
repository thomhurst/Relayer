namespace Relayer;

/// <summary>A lazy view over native message headers. Creating the view reads no headers and allocates nothing.</summary>
public readonly struct MessageHeaders<TIncoming, TReader>(TIncoming message, HeaderNames names)
    where TReader : IMessageHeaderReader<TIncoming>
{
    /// <summary>Looks up one native header on demand.</summary>
    public bool TryGetValue(string name, out WireString value) => TReader.TryGetHeader(message, name, out value);

    /// <summary>Gets the CloudEvents wire type, or the caller's explicit mapping in raw mode.</summary>
    /// <remarks>Raw mode does not inspect CloudEvents headers, even when the native message contains them.</remarks>
    public WireString GetMessageType(MessageEnvelopeMode mode = MessageEnvelopeMode.CloudEvents, WireString rawMessageType = default)
    {
        if (mode == MessageEnvelopeMode.Raw)
        {
            if (rawMessageType.IsEmpty)
            {
                throw new InvalidOperationException("Raw mode requires an explicit message type mapping, for example from the topic name.");
            }

            return rawMessageType;
        }

        if (mode != MessageEnvelopeMode.CloudEvents)
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        if (!TryGetValue(names.Type, out var value) || value.IsEmpty)
        {
            throw new InvalidOperationException($"The message has no '{names.Type}' header. Configure raw mode and an explicit type mapping for legacy producers.");
        }

        return value;
    }
}
