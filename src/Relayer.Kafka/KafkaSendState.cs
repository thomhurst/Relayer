using System.Buffers;
using System.Text;
using Dekaf.Serialization;

namespace Relayer.Kafka;

internal sealed class KafkaKeySerializer : ISerializer<WireString?>
{
    public void Serialize<TWriter>(WireString? value, ref TWriter destination, SerializationContext context)
        where TWriter : IBufferWriter<byte>, allows ref struct
    {
        if (value is not { } bytes) return;
        if (bytes.Text is { } text)
        {
            var size = Encoding.UTF8.GetByteCount(text);
            var written = Encoding.UTF8.GetBytes(text, destination.GetSpan(size));
            destination.Advance(written);
        }
        else
        {
            bytes.Utf8.Span.CopyTo(destination.GetSpan(bytes.Utf8.Length));
            destination.Advance(bytes.Utf8.Length);
        }
    }
}

// Each operation owns its native objects and encoding storage until every native send completes.
internal sealed class KafkaSendState
{
    internal const string TombstoneHeader = "relayer.internal.kafka.tombstone";
    private readonly Headers _headers = new(16);
    private byte[] _utf8 = [];
    internal WireString? Key { get; private set; }
    internal bool Tombstone { get; set; }
    internal Headers Headers => _headers;

    internal void Prepare(OutgoingMessage outgoing)
    {
        Clear();
        var size = 0;
        foreach (var header in outgoing.Headers.Span)
            if (header.Value.Text is { } text) size = checked(size + Encoding.UTF8.GetByteCount(text));
        if (_utf8.Length < size) _utf8 = new byte[Math.Max(size, 256)];
        var position = 0;
        Key = outgoing.OrderingKey;
        foreach (var header in outgoing.Headers.Span)
            _headers.Add(new Header(header.Key, Encode(header.Value, ref position)));
        outgoing.ApplyNative(_headers);
        // The core's serialized copy seam carries transport-specific null-body state through
        // its existing native callback. The marker never reaches the broker.
        if (outgoing.Body.IsEmpty && _headers.GetFirst(TombstoneHeader) is not null)
        {
            Tombstone = true;
            _headers.Remove(TombstoneHeader);
        }
    }

    private ReadOnlyMemory<byte> Encode(WireString value, ref int position)
    {
        if (value.Text is not { } text) return value.Utf8;
        var written = Encoding.UTF8.GetBytes(text, _utf8.AsSpan(position));
        var result = _utf8.AsMemory(position, written);
        position += written;
        return result;
    }

    internal void Clear()
    {
        _headers.Clear();
        Key = null;
        Tombstone = false;
    }
}

// Use Dekaf's native record-header serializer hook so its componentwise single/batch
// overloads need no immutable ProducerMessage allocation. Native tracing starts first;
// serialization then restores original creation headers, retaining other native additions.
internal sealed class KafkaValueSerializer : ISerializer<(ReadOnlyMemory<byte> Body, Headers Headers)?>, IRecordHeaderSerializer
{
    public bool ProducesRecordHeaders => true;
    public void Serialize<TWriter>((ReadOnlyMemory<byte> Body, Headers Headers)? record, ref TWriter destination,
        SerializationContext context) where TWriter : IBufferWriter<byte>, allows ref struct
    {
        var value = record.GetValueOrDefault();
        var headers = context.Headers ?? throw new InvalidOperationException("Dekaf did not provide record header storage.");
        if (value.Headers.GetFirst("traceparent") is not null)
        {
            headers.Remove("traceparent");
            headers.Remove("tracestate");
        }
        // Remove conflicts first so duplicate application headers retain their native order.
        for (var i = 0; i < value.Headers.Count; i++) headers.Remove(value.Headers[i].Key);
        for (var i = 0; i < value.Headers.Count; i++) headers.Add(value.Headers[i]);
        value.Body.Span.CopyTo(destination.GetSpan(value.Body.Length));
        destination.Advance(value.Body.Length);
    }
}
