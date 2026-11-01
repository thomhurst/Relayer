using Dekaf;

namespace Relayer.Kafka;

/// <summary>
/// Options for the Kafka transport, which wraps the Dekaf client. Relayer supplies Dekaf's own builders,
/// so native settings remain available unchanged (docs/decisions.md D1, D22).
/// </summary>
public sealed class KafkaTransportOptions
{
    /// <summary>Bootstrap servers shared by the producer and consumers.</summary>
    public IList<string> BootstrapServers { get; } = [];

    /// <summary>Customises the Dekaf producer builder before the producer is built.</summary>
    /// <remarks>Keep the supplied key and record-header serializers to preserve wire bodies and creation headers.</remarks>
    public Action<ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Dekaf.Serialization.Headers Headers)?>>? ConfigureProducer { get; set; }

    /// <summary>Customises each Dekaf consumer builder. Keep raw byte deserializers; Relayer disables automatic commits and offset storage afterward.</summary>
    public Action<ConsumerBuilder<byte[], byte[]>>? ConfigureConsumer { get; set; }
}
