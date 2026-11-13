using System.Buffers;
using System.Diagnostics;
using System.Text;
using Dekaf.Consumer;
using Dekaf.Serialization;

namespace Relayer.Kafka;

/// <summary>A borrowed Kafka record, valid only while the receive callback runs.</summary>
public readonly record struct KafkaDelivery
{
    private readonly ConsumeResult<byte[], byte[]> _result;
    private readonly IReadOnlyList<Header>? _headers;

    /// <summary>Creates a borrowed delivery with one native header view shared by its copies.</summary>
    public KafkaDelivery(ConsumeResult<byte[], byte[]> Result, string PartitionId)
    {
        _result = Result;
        _headers = Result.Headers;
        this.PartitionId = PartitionId;
    }

    /// <summary>The untouched native record.</summary>
    public ConsumeResult<byte[], byte[]> Result
    {
        get => _result;
        init
        {
            _result = value;
            // A with-expression replacing the record must also replace its header view.
            _headers = value.Headers;
        }
    }

    /// <summary>The native partition identifier.</summary>
    public string PartitionId { get; init; }

    internal IReadOnlyList<Header> Headers => _headers ?? Array.Empty<Header>();

    /// <summary>Deconstructs the native record and partition identifier.</summary>
    public void Deconstruct(out ConsumeResult<byte[], byte[]> Result, out string PartitionId)
        => (Result, PartitionId) = (this.Result, this.PartitionId);

    // The shared native view is an implementation detail, not part of delivery identity.
    /// <inheritdoc />
    public bool Equals(KafkaDelivery other)
        => EqualityComparer<ConsumeResult<byte[], byte[]>>.Default.Equals(Result, other.Result)
            && EqualityComparer<string>.Default.Equals(PartitionId, other.PartitionId);

    /// <inheritdoc />
    public override int GetHashCode()
        => unchecked(EqualityComparer<ConsumeResult<byte[], byte[]>>.Default.GetHashCode(Result) * -1521134295
            + EqualityComparer<string>.Default.GetHashCode(PartitionId!));
}

/// <summary>Reads the native Kafka binary CloudEvents binding using one safe native header snapshot and the borrowed body.</summary>
public readonly struct KafkaMessageBinding : IMessageBinding<KafkaDelivery>
{
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
        var headers = message.Headers;
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
