using System.Buffers;
using System.Collections.Frozen;

namespace Relayer.Serialization.Protobuf;

/// <summary>Serializes raw Protobuf bodies using explicitly registered generated contracts.</summary>
/// <remarks>Instances are immutable and thread-safe. No schema registry framing or reflection fallback is used.</remarks>
public sealed class ProtobufMessageSerializer : IMessageSerializer
{
    private readonly FrozenDictionary<object, object> _codecs;

    /// <summary>Configures the generated parsers once and captures an immutable registry.</summary>
    public ProtobufMessageSerializer(Action<ProtobufSerializerBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new ProtobufSerializerBuilder();
        configure(builder);
        _codecs = builder.Build();
    }

    /// <inheritdoc />
    public string ContentType => "application/protobuf";

    /// <inheritdoc />
    public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(destination);
        GetCodec<TMessage>().Write(message, destination);
    }

    /// <inheritdoc />
    public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => GetCodec<TMessage>().Read(source);

    private ProtobufCodec<TMessage> GetCodec<TMessage>() =>
        _codecs.TryGetValue(ProtobufCodec<TMessage>.Key, out var codec)
            ? (ProtobufCodec<TMessage>)codec
            : throw new InvalidOperationException("No Protobuf parser is registered for this message. Call ProtobufSerializerBuilder.Add with its generated Parser.");
}

internal sealed class ProtobufCodec<TMessage>(Action<TMessage, IBufferWriter<byte>> write, Func<ReadOnlySequence<byte>, TMessage> read)
{
    internal Action<TMessage, IBufferWriter<byte>> Write { get; } = write;
    internal Func<ReadOnlySequence<byte>, TMessage> Read { get; } = read;

    // Immutable identity only; parsers and configuration belong to each serializer instance.
    internal static readonly object Key = new();
}
