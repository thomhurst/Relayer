using System.Diagnostics;
using Dekaf.Consumer;
using Dekaf.Serialization;

namespace Relayer.Kafka.Tests;

public class KafkaBindingTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    public async Task Native_attributes_distinguish_null_keys_and_tombstones(bool nullKey, bool tombstone)
    {
        var incoming = Record(nullKey, tombstone);
        using var activity = new Activity("binding").Start();
        KafkaMessageBinding.EnrichActivity(activity, incoming);
        await Assert.That(activity.GetTagItem("messaging.kafka.offset")).IsEqualTo(42L);
        await Assert.That(activity.GetTagItem("messaging.kafka.message.key")).IsEqualTo(nullKey ? null : "customer");
        await Assert.That(activity.GetTagItem("messaging.kafka.message.tombstone")).IsEqualTo(tombstone ? true : null);
        await Assert.That(KafkaMessageBinding.GetDeliveryCount(incoming)).IsNull();
        await Assert.That(KafkaMessageBinding.GetPartitionId(incoming)).IsEqualTo("0");
    }

    [Test]
    public async Task Caller_owned_header_binding_reuses_storage_without_allocating()
    {
        // The public native constructor uses caller-owned headers; live fetch batches
        // materialize native snapshots, covered by KafkaHeaderAllocationBenchmarks.
        var incoming = Record(false, false);
        for (var i = 0; i < 1000; i++) Read(incoming);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) Read(incoming);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(KafkaMessageBinding.GetBody(incoming).First.Equals(incoming.Result.Value.AsMemory())).IsTrue();
    }

    private static void Read(KafkaDelivery incoming)
    {
        _ = KafkaMessageBinding.GetBody(incoming);
        _ = KafkaMessageBinding.GetPartitionId(incoming);
        _ = KafkaMessageBinding.TryGetHeader(incoming, "ce_id", out _);
    }

    private static KafkaDelivery Record(bool nullKey, bool tombstone) => new(new ConsumeResult<byte[], byte[]>(
        "orders", 0, 42, "customer"u8.ToArray(), nullKey, "{}"u8.ToArray(), tombstone,
        new Headers().Add("ce_id", "id"), 0, TimestampType.CreateTime, null, Serializers.ByteArray, Serializers.ByteArray), "0");
}
