using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using MongoDB.Bson;
using MongoDB.Bson.IO;

namespace Relayer.Outbox.MongoDB;

// Write BSON into a reusable buffer. No class maps, per-header arrays or body-sized temporary copies.
// The native insert must finish before the next write; RawBsonDocument borrows this buffer.
internal sealed class MongoDBOutboxBuffer
{
    private readonly ArrayBufferWriter<byte> _buffer = new();

    internal RawBsonDocument Write(OutboxRecord record, int lanes)
    {
        Encode(record, lanes);
        MemoryMarshal.TryGetArray(_buffer.WrittenMemory, out var segment);
        return new RawBsonDocument(new ByteArrayBuffer(segment.Array!, _buffer.WrittenCount, isReadOnly: true));
    }

    internal int Encode(OutboxRecord record, int lanes)
    {
        record.Message.ValidateForOutbox();
        if (record.Id == Guid.Empty || record.Id.Version != 7 || string.IsNullOrWhiteSpace(record.MessageType))
            throw new ArgumentException("Outbox sends require the original UUIDv7 ID and stable MessageType.", nameof(record));
        if (record.Lane is not null || record.Attempts != 0 || record.LastError is not null)
            throw new ArgumentException("New sends cannot specify lanes, attempts or errors; lanes come from the ordering key.", nameof(record));
        ArgumentException.ThrowIfNullOrWhiteSpace(record.TransportName);
        ArgumentException.ThrowIfNullOrWhiteSpace(record.Message.Destination);
        _buffer.Clear();
        Int32(0); // Document size, patched after all growth.
        Element(5, "_id"); Int32(16); Byte(4);
        record.Id.TryWriteBytes(_buffer.GetSpan(16), bigEndian: true, out _); _buffer.Advance(16);
        Text("transport", record.TransportName);
        Text("message_type", record.MessageType);
        Text("destination", record.Message.Destination);
        Binary("body", record.Message.Body.Span);
        // A binary length-prefixed list preserves duplicate names, empty and arbitrary binary values.
        Element(5, "headers");
        var sizePosition = _buffer.WrittenCount;
        Int32(0); Byte(0);
        Int32(record.Message.Headers.Length);
        foreach (var header in record.Message.Headers.Span)
        {
            ArgumentNullException.ThrowIfNull(header.Key);
            var nameLength = Encoding.UTF8.GetByteCount(header.Key);
            Int32(nameLength); Utf8(header.Key);
            WriteWireValue(header.Value);
        }
        PatchInt32(sizePosition, _buffer.WrittenCount - sizePosition - 5);
        int? lane = null;
        if (record.Message.OrderingKey is { } key)
        {
            Element(5, "ordering_key");
            var keyPosition = _buffer.WrittenCount;
            WriteWireValue(key, bson: true);
            var keyBytes = _buffer.WrittenSpan[(keyPosition + 5)..];
            uint hash = 2166136261;
            foreach (var value in keyBytes) hash = unchecked((hash ^ value) * 16777619);
            lane = (int)(hash % (uint)lanes);
        }
        else Null("ordering_key");
        if (lane is { } laneValue) { Element(16, "lane"); Int32(laneValue); }
        else Null("lane");
        Ticks("created_at_utc_ticks", record.CreatedAt);
        if (record.Message.DeliverAt is { } due) Ticks("deliver_at_utc_ticks", due);
        else Null("deliver_at_utc_ticks");
        if (record.TenantId is { } tenant) Text("tenant_id", tenant); else Null("tenant_id");
        Text("status", "pending");
        Element(16, "attempts"); Int32(0);
        Null("last_error"); Null("claim_token"); Null("claim_expires_at_utc_ticks");
        Byte(0);
        PatchInt32(0, _buffer.WrittenCount);
        return _buffer.WrittenCount;
    }

    private void WriteWireValue(WireString value, bool bson = false)
    {
        var size = value.Text is { } text ? Encoding.UTF8.GetByteCount(text) : value.Utf8.Length;
        Int32(size);
        if (bson) Byte(0);
        if (value.Text is { } content) Utf8(content); else _buffer.Write(value.Utf8.Span);
    }

    private void Text(string name, string value)
    {
        Element(2, name); Int32(Encoding.UTF8.GetByteCount(value) + 1); Utf8(value); Byte(0);
    }

    private void Binary(string name, ReadOnlySpan<byte> value)
    {
        Element(5, name); Int32(value.Length); Byte(0); _buffer.Write(value);
    }

    private void Ticks(string name, DateTimeOffset value)
    {
        Element(18, name); BinaryPrimitives.WriteInt64LittleEndian(_buffer.GetSpan(8), value.UtcTicks); _buffer.Advance(8);
    }

    private void Null(string name) => Element(10, name);
    private void Element(byte type, string name) { Byte(type); Utf8(name); Byte(0); }
    private void Utf8(string value) => _buffer.Advance(Encoding.UTF8.GetBytes(value, _buffer.GetSpan(Encoding.UTF8.GetByteCount(value))));
    private void Byte(byte value) { _buffer.GetSpan(1)[0] = value; _buffer.Advance(1); }
    private void Int32(int value) { BinaryPrimitives.WriteInt32LittleEndian(_buffer.GetSpan(4), value); _buffer.Advance(4); }
    private void PatchInt32(int offset, int value)
    {
        MemoryMarshal.TryGetArray(_buffer.WrittenMemory, out var segment);
        BinaryPrimitives.WriteInt32LittleEndian(segment.Array!.AsSpan(offset, 4), value);
    }
}
