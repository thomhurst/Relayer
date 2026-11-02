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

    internal void Copy(OutboxRecord record, int lanes)
    {
        record.Id.TryWriteBytes((byte[])Values[0], bigEndian: true, out _);
        var message = record.Message;
        Values[2] = DBNull.Value;
        _hasLane = message.OrderingKey is not null;
        if (message.OrderingKey is { } key)
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

    internal object GetValue(int ordinal) => ordinal switch
    {
        1 when !_hasLane => DBNull.Value,
        9 when !_hasDelivery => DBNull.Value,
        _ => Values[ordinal]
    };

    internal void Clear()
    {
        Values[2] = Values[10] = DBNull.Value;
        Values[3] = Values[4] = Values[5] = "";
        _headers.Clear();
        _key.SetLength(0);
        ((MemoryStream)Values[6]).SetLength(0);
        ((MemoryStream)Values[7]).SetLength(0);
    }
}
