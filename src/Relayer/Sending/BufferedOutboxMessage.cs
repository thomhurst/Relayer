using System.Buffers;
using System.Text;

namespace Relayer;

internal sealed class BufferedOutboxMessage
{
    private readonly ArrayBufferWriter<byte> _buffer = new();
    private KeyValuePair<string, WireString>[] _decodedHeaders = [];
    internal string Transport { get; private set; } = "";
    internal string Destination { get; private set; } = "";
    internal string MessageType { get; private set; } = "";
    internal Guid Id { get; private set; }
    // Scope-local enqueue position when several EF contexts share one transaction.
    internal long EnqueueOrder { get; set; }
    internal DateTimeOffset CreatedAt { get; private set; }
    internal string? TenantId { get; private set; }
    internal DateTimeOffset? DeliverAt { get; private set; }
    internal int? Lane { get; private set; }
    internal ReadOnlyMemory<byte> Body { get; private set; }
    internal ReadOnlyMemory<byte> Headers { get; private set; }
    internal ReadOnlyMemory<byte>? OrderingKey { get; private set; }

    internal static void Validate(OutboxRecord record)
    {
        record.Message.ValidateForOutbox();
        if (record.Id == Guid.Empty || record.Id.Version != 7 || string.IsNullOrWhiteSpace(record.MessageType))
            throw new InvalidOperationException("Outbox sends require the original UUIDv7 identity and stable MessageType. Use ISender or supply the original OutboxRecord metadata.");
        if (record.Attempts != 0 || record.LastError is not null || record.Lane is not null)
            throw new ArgumentException("New outbox sends cannot specify dispatch attempts, errors or lanes. Lanes are computed from the ordering key.", nameof(record));
        ArgumentException.ThrowIfNullOrWhiteSpace(record.TransportName);
        ArgumentException.ThrowIfNullOrWhiteSpace(record.Message.Destination);
    }

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
        DurableOutboxHeaderCodec.Write(_buffer, message.Headers.Span);
        var headerEnd = _buffer.WrittenCount;
        if (message.OrderingKey is { } key) WriteValue(key);
        // Take slices only after growth has finished: ArrayBufferWriter can replace its backing array.
        Body = _buffer.WrittenMemory[..bodyLength];
        Headers = _buffer.WrittenMemory[bodyLength..headerEnd];
        OrderingKey = message.OrderingKey is null ? (ReadOnlyMemory<byte>?)null : _buffer.WrittenMemory[headerEnd..];
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

    internal OutgoingMessage ToMessage() => new OutgoingMessage(Destination, Body,
        DurableOutboxHeaderCodec.Read(Headers, ref _decodedHeaders),
        OrderingKey is { } key ? new WireString(key) : null).ForOutboxDispatch();

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
        // Retain decoded names for the codec's next use, but release all borrowed payload slices.
        for (var i = 0; i < _decodedHeaders.Length; i++)
            _decodedHeaders[i] = new(_decodedHeaders[i].Key, default);
        _buffer.Clear();
    }
}
