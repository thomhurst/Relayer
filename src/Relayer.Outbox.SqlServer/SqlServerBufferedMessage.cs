using System.Buffers;
using System.Data.SqlTypes;
using System.Runtime.CompilerServices;
using System.Text;

namespace Relayer.Outbox.SqlServer;

// All object-valued native fields and payload storage survive successive units in the same scope.
internal sealed class SqlServerBufferedMessage
{
    internal readonly object[] Values = [new byte[16], 0, DBNull.Value, "", "", "", new MemoryStream(), new MemoryStream(), default(DateTimeOffset), default(DateTimeOffset), DBNull.Value];
    private readonly ArrayBufferWriter<byte> _headers = new();
    private SqlChars _key = new([]);
    private KeyValuePair<string, WireString>[] _decodedHeaders = [];
    private bool _hasLane;
    private bool _hasDelivery;

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

    internal void Copy(OutboxRecord record, int lanes, bool unordered = false)
    {
        record.Id.TryWriteBytes((byte[])Values[0], bigEndian: true, out _);
        var message = record.Message;
        Values[2] = DBNull.Value;
        _hasLane = !unordered && message.OrderingKey is not null;
        if (!unordered && message.OrderingKey is { } key)
        {
            // Hash exactly the same UTF-8 bytes as PostgreSQL, including keys supplied as text.
            _headers.Clear();
            if (key.Text is { } text)
            {
                var length = Encoding.UTF8.GetByteCount(text);
                Encoding.UTF8.GetBytes(text, _headers.GetSpan(length));
                _headers.Advance(length);
            }
            else _headers.Write(key.Utf8.Span);
            uint hash = 2166136261;
            foreach (var value in _headers.WrittenSpan) hash = unchecked((hash ^ value) * 16777619);
            Unsafe.Unbox<int>(Values[1]) = (int)(hash % (uint)lanes);
            var count = Encoding.UTF8.GetCharCount(_headers.WrittenSpan);
            if (_key.MaxLength < count) _key = new(new char[count]);
            Encoding.UTF8.GetChars(_headers.WrittenSpan, _key.Buffer);
            _key.SetLength(count);
            Values[2] = _key;
        }
        Values[3] = record.TransportName;
        Values[4] = message.Destination;
        Values[5] = record.MessageType;
        _headers.Clear();
        DurableOutboxHeaderCodec.Write(_headers, message.Headers.Span);
        CopyBytes(6, _headers.WrittenSpan);
        CopyBytes(7, message.Body.Span);
        Unsafe.Unbox<DateTimeOffset>(Values[8]) = record.CreatedAt.ToUniversalTime();
        _hasDelivery = message.DeliverAt.HasValue;
        Unsafe.Unbox<DateTimeOffset>(Values[9]) = message.DeliverAt.GetValueOrDefault().ToUniversalTime();
        Values[10] = (object?)record.TenantId ?? DBNull.Value;
    }

    private void CopyBytes(int ordinal, ReadOnlySpan<byte> source)
    {
        var stream = (MemoryStream)Values[ordinal];
        stream.SetLength(0);
        stream.Write(source);
        stream.Position = 0;
    }

    internal Guid Id => new((byte[])Values[0], bigEndian: true);
    internal string Transport => (string)Values[3];
    internal string Destination => (string)Values[4];
    internal DateTimeOffset CreatedAt => (DateTimeOffset)Values[8];
    internal DateTimeOffset? DeliverAt => _hasDelivery ? (DateTimeOffset)Values[9] : null;
    internal int? Lane => _hasLane ? (int)Values[1] : null;

    internal OutgoingMessage ToMessage()
    {
        var headers = (MemoryStream)Values[6];
        var body = (MemoryStream)Values[7];
        return new OutgoingMessage(Destination, body.GetBuffer().AsMemory(0, checked((int)body.Length)),
            DurableOutboxHeaderCodec.Read(headers.GetBuffer().AsMemory(0, checked((int)headers.Length)), ref _decodedHeaders))
            .ForOutboxDispatch();
    }

    internal object GetValue(int ordinal) => ordinal switch
    {
        1 when !_hasLane => DBNull.Value,
        9 when !_hasDelivery => DBNull.Value,
        _ => Values[ordinal]
    };

    internal void Clear()
    {
        for (var i = 0; i < _decodedHeaders.Length; i++)
            _decodedHeaders[i] = new(_decodedHeaders[i].Key, default);
        _hasLane = _hasDelivery = false;
        Values[2] = Values[10] = DBNull.Value;
        Values[3] = Values[4] = Values[5] = "";
        _headers.Clear();
        _key.SetLength(0);
        ((MemoryStream)Values[6]).SetLength(0);
        ((MemoryStream)Values[7]).SetLength(0);
    }
}
