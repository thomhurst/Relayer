using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Dekaf.Consumer;
using Dekaf.Serialization;

namespace Relayer.Kafka;

/// <summary>A borrowed Kafka record, valid only while the receive callback runs.</summary>
public readonly record struct KafkaDelivery(ConsumeResult<byte[], byte[]> Result, string PartitionId)
{
    internal static KafkaDelivery Snapshot(ConsumeResult<byte[], byte[]> record, string partitionId)
    {
        // Dekaf owns deserialized byte arrays. Materialize its native header snapshot once,
        // before advancing the fetch window, and retain that list with those same arrays.
        var headers = record.Headers;
        if (headers.Count != 0) _ = headers[0];
        return new(new(record.Topic, record.Partition, record.Offset, record.Key, record.IsKeyNull,
            record.Value, record.Value is null, headers, record.TimestampMs, record.TimestampType,
            record.LeaderEpoch, OwnedBytes.Instance, OwnedBytes.Instance), partitionId);
    }

    private sealed class OwnedBytes : IDeserializer<byte[]>
    {
        internal static readonly OwnedBytes Instance = new();
        public byte[] Deserialize(ReadOnlyMemory<byte> data, SerializationContext context)
        {
            if (context.IsNull) return null!;
            if (MemoryMarshal.TryGetArray(data, out var segment) && segment.Offset == 0 && segment.Count == segment.Array!.Length)
                return segment.Array;
            throw new InvalidOperationException("Kafka delivery snapshots require native byte-array deserialization.");
        }
    }
}

/// <summary>Reads the native Kafka binary CloudEvents binding without copying headers or body.</summary>
public readonly struct KafkaMessageBinding : IPartitionMessageBinding<KafkaDelivery>
{
    /// <summary>Compares native key bytes without decoding. Null keys share their own ordered lane.</summary>
    public static bool KeysEqual(KafkaDelivery first, KafkaDelivery second) =>
        first.Result.IsKeyNull == second.Result.IsKeyNull &&
        (first.Result.IsKeyNull || first.Result.Key.AsSpan().SequenceEqual(second.Result.Key));
    /// <inheritdoc />
    public static ReadOnlySequence<byte> GetBody(KafkaDelivery message) => new(message.Result.Value ?? []);
    /// <inheritdoc />
    public static bool IsTombstone(KafkaDelivery message) => message.Result.Value is null;
    /// <inheritdoc />
    public static WireString? GetTombstoneKey(KafkaDelivery message) => message.Result.IsKeyNull ? null : new WireString(message.Result.Key!);
    /// <inheritdoc />
    public static int? GetDeliveryCount(KafkaDelivery message) => null;
    /// <inheritdoc />
    public static string GetPartitionId(KafkaDelivery message) => message.PartitionId;
    /// <inheritdoc />
    public static bool TryGetHeader(KafkaDelivery message, string name, out WireString value)
    {
        var headers = message.Result.Headers;
        for (var i = 0; i < headers.Count; i++)
        {
            if (headers[i].Key != name) continue;
            value = new(headers[i].Value);
            return true;
        }
        value = default;
        return false;
    }
    /// <inheritdoc />
    public static void EnrichActivity(Activity activity, KafkaDelivery message)
    {
        var record = message.Result;
        activity.SetTag("messaging.kafka.offset", record.Offset);
        if (!record.IsKeyNull) activity.SetTag("messaging.kafka.message.key", Encoding.UTF8.GetString(record.Key!));
        if (record.Value is null) activity.SetTag("messaging.kafka.message.tombstone", true);
    }
}

/// <summary>A struct view over the pooled native feature. Do not retain it beyond processing.</summary>
public readonly struct KafkaConsumeFeature
{
    private readonly NativeMessage<KafkaDelivery> _native;
    internal KafkaConsumeFeature(NativeMessage<KafkaDelivery> native) => _native = native;
    /// <summary>The native Dekaf record, including its headers, key, partition and offset.</summary>
    public ConsumeResult<byte[], byte[]> ConsumeResult => _native.Message.Result;
}

/// <summary>Typed access to the current native Kafka delivery.</summary>
public static class KafkaContextExtensions
{
    /// <summary>Returns a borrowed view of the native record; throws for another transport.</summary>
    public static KafkaConsumeFeature Kafka(this MessageContext context) => new(context.Features.GetRequired<NativeMessage<KafkaDelivery>>());
}
