using System.Buffers;
using System.Text;

namespace Relayer.Outbox.PostgreSql;

internal sealed class BufferedOutboxMessage
{
    private readonly ArrayBufferWriter<byte> _buffer = new();
    internal string Transport { get; private set; } = "";
    internal string Destination { get; private set; } = "";
    internal string MessageType { get; private set; } = "";
    internal Guid Id { get; private set; }
    internal DateTimeOffset CreatedAt { get; private set; }
    internal string? TenantId { get; private set; }
    internal DateTimeOffset? DeliverAt { get; private set; }
    internal int? Lane { get; private set; }
    internal ReadOnlyMemory<byte> Body { get; private set; }
    internal ReadOnlyMemory<byte> Headers { get; private set; }
    internal ReadOnlyMemory<byte>? OrderingKey { get; private set; }

    internal void Copy(OutboxRecord record, int lanes)
    {
        var message = record.Message;
        _buffer.Clear();
        Transport = record.TransportName;
        Destination = message.Destination;
        MessageType = record.MessageType;
        Id = record.Id;
        CreatedAt = record.CreatedAt.ToUniversalTime();
        TenantId = record.TenantId;
        DeliverAt = message.DeliverAt?.ToUniversalTime();
        _buffer.Write(message.Body.Span);
        var bodyLength = _buffer.WrittenCount;
        OutboxHeaderCodec.Write(_buffer, message.Headers.Span);
        var headerEnd = _buffer.WrittenCount;
        if (message.OrderingKey is { } key) WriteValue(key);
        // Take slices only after growth has finished: ArrayBufferWriter can replace its backing array.
        Body = _buffer.WrittenMemory[..bodyLength];
        Headers = _buffer.WrittenMemory[bodyLength..headerEnd];
        OrderingKey = message.OrderingKey is null ? null : _buffer.WrittenMemory[headerEnd..];
        Lane = null;
        if (OrderingKey is { } orderingKey)
        {
            uint hash = 2166136261;
            foreach (var value in orderingKey.Span) hash = unchecked((hash ^ value) * 16777619);
            Lane = (int)(hash % (uint)lanes);
        }
    }

    private void WriteValue(WireString value)
    {
        var length = value.Text is { } text ? Encoding.UTF8.GetByteCount(text) : value.Utf8.Length;
        if (value.Text is { } stringValue)
        {
            Encoding.UTF8.GetBytes(stringValue, _buffer.GetSpan(length));
            _buffer.Advance(length);
        }
        else _buffer.Write(value.Utf8.Span);
    }

    internal void Clear()
    {
        Transport = Destination = MessageType = "";
        Id = default;
        CreatedAt = default;
        TenantId = null;
        DeliverAt = null;
        Lane = null;
        Body = Headers = default;
        OrderingKey = null;
        _buffer.Clear();
    }
}
