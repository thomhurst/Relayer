using Dekaf;

namespace Relayer.Kafka;

/// <summary>
/// Options for the Kafka transport, which wraps the Dekaf client. Relayer creates the producer and consumers
/// and then hands you Dekaf's own builders, so every Dekaf setting is available unchanged (docs/decisions.md D1, D22).
/// </summary>
public sealed class KafkaTransportOptions
{
    /// <summary>Bootstrap servers shared by the producer and consumers.</summary>
    public IList<string> BootstrapServers { get; } = [];

    /// <summary>Customises the Dekaf producer builder before the producer is built.</summary>
    public Action<ProducerBuilder<byte[], byte[]>>? ConfigureProducer { get; set; }

    /// <summary>Customises each Dekaf consumer builder before the consumer is built.</summary>
    public Action<ConsumerBuilder<byte[], byte[]>>? ConfigureConsumer { get; set; }
}
