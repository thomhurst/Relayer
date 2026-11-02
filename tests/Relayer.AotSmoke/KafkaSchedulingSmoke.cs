using Dekaf;
using Dekaf.Consumer;
using Dekaf.Serialization;
using Relayer;
using Relayer.Kafka;

internal static class KafkaSchedulingSmoke
{
    internal static async Task RunAsync()
    {
        var wake = new KafkaPumpSignal();
        var completion = new TaskCompletionSource<Disposition>();
        var source = new Source();
        var scheduler = new PartitionScheduler<KafkaDelivery, KafkaMessageBinding, TopicPartition, TopicPartitionOffset>(
            source, (_, _) => new(completion.Task), completionReady: wake.Signal);
        var partition = new TopicPartition("aot", 0);
        var assignment = scheduler.Assign(partition);
        var original = new ConsumeResult<byte[], byte[]>("aot", 0, 91, "key"u8.ToArray(), false,
            "{}"u8.ToArray(), false, new Headers().Add("ce_id", "aot"), 0, TimestampType.CreateTime, 4,
            Serializers.ByteArray, Serializers.ByteArray);
        var delivery = KafkaDelivery.Snapshot(original, "0");
        if (!ReferenceEquals(delivery.Result.Value, original.Value) || !KafkaMessageBinding.KeysEqual(delivery, delivery) ||
            !scheduler.TryEnqueue(assignment, delivery, new("aot", 0, 92, 4)))
            throw new InvalidOperationException("Kafka native snapshot failed under Native AOT.");
        scheduler.Pump();
        completion.SetResult(Disposition.Complete);
        await wake.WaitAsync();
        scheduler.Pump();
        if (source.Checkpoint.Offset != 92 || source.Checkpoint.LeaderEpoch != 4 || scheduler.BufferedCount != 0)
            throw new InvalidOperationException("Kafka scheduled completion failed under Native AOT.");
    }

    private sealed class Source : IPartitionSource<TopicPartition, TopicPartitionOffset>
    {
        internal TopicPartitionOffset Checkpoint;
        public void Commit(TopicPartition partition, TopicPartitionOffset position) => Checkpoint = position;
        public void Pause(TopicPartition partition) { }
        public void Resume(TopicPartition partition) { }
    }
}
