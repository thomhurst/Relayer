using System.Buffers;
using MessagePack;

namespace Relayer.Serialization.MessagePack;

/// <summary>Serializes MessagePack bodies using explicitly supplied native options.</summary>
/// <remarks>
/// Use explicit generated and primitive formatters for Native AOT and trimming. The adapter never selects a resolver
/// or changes global defaults. Custom resolvers and formatters must be thread-safe.
/// </remarks>
public sealed class MessagePackMessageSerializer : IMessageSerializer
{
    private readonly MessagePackSerializerOptions _options;

    /// <summary>Captures native formatter options; compression must be disabled. No resolver fallback is added.</summary>
    public MessagePackMessageSerializer(MessagePackSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Compression != MessagePackCompression.None)
            throw new ArgumentException("MessagePack compression is not supported by the AOT-safe formatter adapter. Use MessagePackCompression.None.", nameof(options));
        _options = options;
    }

    /// <inheritdoc />
    public string ContentType => "application/x-msgpack";

    /// <inheritdoc />
    public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(destination);
        var writer = new MessagePackWriter(destination) { OldSpec = _options.OldSpec ?? false };
        try
        {
            _options.Resolver.GetFormatterWithVerify<TMessage>().Serialize(ref writer, message, _options);
            writer.Flush();
        }
        catch (Exception error)
        {
            throw new MessagePackSerializationException("Failed to serialize the message body.", error);
        }
    }

    /// <inheritdoc />
    public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
    {
        var reader = new MessagePackReader(source);
        TMessage message;
        try
        {
            message = _options.Resolver.GetFormatterWithVerify<TMessage>().Deserialize(ref reader, _options);
        }
        catch (Exception error)
        {
            throw new MessagePackSerializationException("Failed to deserialize the message body.", error);
        }
        if (message is null) throw new MessagePackSerializationException("A message body cannot be nil.");
        if (!reader.End) throw new MessagePackSerializationException("A message body must contain exactly one MessagePack value.");
        return message;
    }
}
