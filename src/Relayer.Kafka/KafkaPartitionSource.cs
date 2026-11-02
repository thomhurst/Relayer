using System.Globalization;
using Dekaf;
using Dekaf.Consumer;

namespace Relayer.Kafka;

internal readonly record struct KafkaCheckpoint(TopicPartitionOffset Offset, long Sequence);

// Only the receive pump reads or writes assignment/checkpoint state. Native callbacks enter
// through KafkaOwnership's command queue, so commits cannot race loss or reassignment.
internal sealed class KafkaPartitionSource(IKafkaConsumer<byte[], byte[]> native) : IPartitionSource<TopicPartition, KafkaCheckpoint>
{
    internal sealed class Assignment(TopicPartition partition)
    {
        internal readonly TopicPartition[] NativePartitions = [partition];
        internal readonly string PartitionId = partition.Partition.ToString(CultureInfo.InvariantCulture);
        internal PartitionAssignmentToken Token;
        internal long Delivered;
        internal long Durable;
        internal long Committed;
        internal TopicPartitionOffset Safe;
        internal KafkaCheckpoint Tail;
        internal bool Pending;
        internal bool Revoking;
        internal TopicPartitionOffset? FirstOffset;
    }

    internal readonly Dictionary<TopicPartition, Assignment> Assignments = [];
    internal readonly List<TopicPartitionOffset> Offsets = [];

    public void Commit(TopicPartition partition, KafkaCheckpoint position)
    {
        var assignment = Assignments[partition];
        assignment.Safe = position.Sequence == assignment.Tail.Sequence ? assignment.Tail.Offset : position.Offset;
        assignment.Durable = position.Sequence;
        assignment.Pending = true;
    }

    internal void CaptureTail(Assignment assignment, TopicPartitionOffset offset)
    {
        assignment.Tail = new(offset, assignment.Delivered);
        if (assignment.Durable != assignment.Delivered) return;
        assignment.Safe = offset;
        assignment.Pending = true;
    }

    public void Pause(TopicPartition partition) => native.Pause(Assignments[partition].NativePartitions);
    public void Resume(TopicPartition partition) => native.Resume(Assignments[partition].NativePartitions);
}
