using System.Collections.Frozen;
using Google.Protobuf;

namespace Relayer.Serialization.Protobuf;

/// <summary>Explicitly registers generated Protobuf parsers at configuration time.</summary>
public sealed class ProtobufSerializerBuilder
{
    private readonly Dictionary<object, object> _codecs = new(ReferenceEqualityComparer.Instance);

    internal ProtobufSerializerBuilder() { }

    /// <summary>Registers a generated parser, preserving its unknown-field and extension settings.</summary>
    /// <remarks>Contracts must implement the generated buffer API (protoc 3.13 or newer).
    /// Duplicate registrations fail during configuration.</remarks>
    public ProtobufSerializerBuilder Add<TMessage>(MessageParser<TMessage> parser)
        where TMessage : class, IMessage<TMessage>, IBufferMessage
    {
        ArgumentNullException.ThrowIfNull(parser);
        if (!_codecs.TryAdd(ProtobufCodec<TMessage>.Key,
                new ProtobufCodec<TMessage>(static (message, destination) => message.WriteTo(destination), parser.ParseFrom)))
        {
            throw new InvalidOperationException("A Protobuf parser is already registered for this message.");
        }
        return this;
    }

    internal FrozenDictionary<object, object> Build() => _codecs.ToFrozenDictionary(ReferenceEqualityComparer.Instance);
}
