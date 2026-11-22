using Dekaf;
using Dekaf.Admin;

namespace Relayer.Kafka;

/// <summary>
/// Options for the Kafka transport, which wraps the Dekaf client. Relayer supplies Dekaf's own builders,
/// so native settings remain available unchanged (docs/decisions.md D1, D22).
/// </summary>
public sealed class KafkaTransportOptions
{
    /// <summary>Bootstrap servers shared by the producer and consumers.</summary>
    public IList<string> BootstrapServers
    {
        get;
    } = [];

    /// <summary>Customises the Dekaf producer builder before the producer is built.</summary>
    /// <remarks>Keep the supplied key and record-header serializers to preserve wire bodies and creation headers.</remarks>
    public Action<ProducerBuilder<WireString?, (ReadOnlyMemory<byte> Body, Dekaf.Serialization.Headers Headers)?>>? ConfigureProducer
    {
        get; set;
    }

    /// <summary>Customises the native admin client used by topology validation, provisioning and dead-letter management, including authentication.</summary>
    public Action<AdminClientBuilder>? ConfigureAdmin
    {
        get; set;
    }

    /// <summary>Native topic declarations, passed unchanged to Dekaf when provisioning missing topics.</summary>
    /// <remarks>
    /// Validate and Provision check declared settings without modifying existing topics. Routes and consumers without
    /// explicit declarations receive existence checks; Provision creates their missing topics with Dekaf's defaults.
    /// </remarks>
    public IList<NewTopic> Topics
    {
        get;
    } = [];

    /// <summary>Native topic creation options. Null uses Dekaf's defaults; Provision requires ValidateOnly to be false.</summary>
    /// <remarks>TimeoutMs also bounds waiting for newly created topics to become visible before checking their settings.</remarks>
    public CreateTopicsOptions? TopicCreationOptions
    {
        get; set;
    }

    /// <summary>Customises each Dekaf consumer builder. Keep raw byte deserializers; Relayer disables automatic commits and offset storage afterward.</summary>
    /// <remarks>Hosted consumption and dead-letter redrive replace any subscription set by this callback with their declared source.</remarks>
    public Action<ConsumerBuilder<byte[], byte[]>>? ConfigureConsumer
    {
        get; set;
    }

    /// <summary>
    /// Starts the consumers with the host. Defaults to true. When false, the host composes and validates the consumers
    /// and starts the producer, but leaves the consumers stopped until the keyed
    /// <see cref="Relayer.Transports.IConsumerControl"/> for this transport starts them.
    /// </summary>
    public bool AutoStartConsumers
    {
        get; set;
    } = true;
}
