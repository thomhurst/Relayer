using Dekaf;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Kafka;

/// <summary>Operational replay for consumers on one named Kafka transport.</summary>
/// <remarks>Call from outside consumer handlers. Seeks drain the current batch first.</remarks>
public interface IKafkaConsumerControl
{
    /// <summary>Moves an assigned partition to a native offset without committing that offset.</summary>
    ValueTask SeekAsync(string group, TopicPartitionOffset offset, CancellationToken cancellationToken = default);

    /// <summary>Looks up a native timestamp offset and seeks to it. Fails when Kafka returns no matching offset.</summary>
    ValueTask SeekAsync(string group, TopicPartitionTimestamp timestamp, CancellationToken cancellationToken = default);
}

internal sealed class KafkaConsumerControl(IServiceProvider provider, string name) : IKafkaConsumerControl
{
    public ValueTask SeekAsync(string group, TopicPartitionOffset offset, CancellationToken cancellationToken = default) =>
        Find(group, offset.Topic, cancellationToken).SeekAsync(offset, null, cancellationToken);

    public ValueTask SeekAsync(string group, TopicPartitionTimestamp timestamp, CancellationToken cancellationToken = default) =>
        Find(group, timestamp.Topic, cancellationToken).SeekAsync(
            new(timestamp.Topic, timestamp.Partition, 0), timestamp, cancellationToken);

    private KafkaConsumer Find(string group, string topic, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var transport = provider.GetRequiredKeyedService<KafkaProducerTransport>(name);
        KafkaConsumer? match = null;
        foreach (var consumer in transport.Consumers)
        {
            if (consumer.Group != group || consumer.Topic != topic) continue;
            if (match is not null)
                throw new InvalidOperationException($"Kafka transport '{name}': multiple consumer declarations match group '{group}' and topic '{topic}'. Declare one consumer for this target to use operational replay.");
            match = consumer;
        }
        return match ?? throw new InvalidOperationException($"Kafka transport '{name}': no running consumer for group '{group}' and topic '{topic}'. Start the host before seeking.");
    }
}
