using Confluent.Kafka;

namespace Relayer.Kafka;

/// <summary>Options for the Kafka transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class KafkaTransportOptions
{
    /// <summary>Native producer configuration, passed straight to Confluent.Kafka.</summary>
    public ProducerConfig Producer { get; set; } = new();

    /// <summary>Native consumer configuration, passed straight to Confluent.Kafka.</summary>
    public ConsumerConfig Consumer { get; set; } = new();
}
