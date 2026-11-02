using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Relayer;

// Versioned lengths preserve duplicate names, order, empty values and arbitrary UTF-8 bytes.
internal static class DurableOutboxHeaderCodec
{
    internal static void Write(IBufferWriter<byte> writer, ReadOnlySpan<KeyValuePair<string, WireString>> headers)
    {
        WriteInt(writer, 1);
        WriteInt(writer, headers.Length);
        foreach (var header in headers)
        {
            WriteString(writer, new WireString(header.Key));
            WriteString(writer, header.Value);
        }
    }

    private static void WriteString(IBufferWriter<byte> writer, WireString value)
    {
        var length = value.Text is { } text ? Encoding.UTF8.GetByteCount(text) : value.Utf8.Length;
        WriteInt(writer, length);
        if (value.Text is { } characters) Encoding.UTF8.GetBytes(characters, writer.GetSpan(length));
        else value.Utf8.Span.CopyTo(writer.GetSpan(length));
        writer.Advance(length);
    }

    private static void WriteInt(IBufferWriter<byte> writer, int value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(writer.GetSpan(4), value);
        writer.Advance(4);
    }

    internal static ReadOnlyMemory<KeyValuePair<string, WireString>> Read(ReadOnlyMemory<byte> data,
        ref KeyValuePair<string, WireString>[] entries)
    {
        var position = 0;
        if (ReadInt(data.Span, ref position) != 1) throw new InvalidDataException("Unsupported outbox header encoding version.");
        var count = ReadInt(data.Span, ref position);
        if (count < 0 || count > (data.Length - position) / 8) throw new InvalidDataException("Invalid outbox header count.");
        if (entries.Length < count) Array.Resize(ref entries, count);
        for (var i = 0; i < count; i++)
        {
            var name = ReadBytes(data, ref position);
            // Header names normally repeat between reads; retain the previous decoded name at each slot.
            var previous = entries[i].Key;
            var key = previous is not null && Matches(previous, name.Span) ? previous : Encoding.UTF8.GetString(name.Span);
            entries[i] = new(key, new WireString(ReadBytes(data, ref position)));
        }
        if (position != data.Length) throw new InvalidDataException("Trailing outbox header bytes.");
        for (var i = count; i < entries.Length; i++) entries[i] = default;
        return entries.AsMemory(0, count);
    }

    private static bool Matches(string text, ReadOnlySpan<byte> utf8)
    {
        if (Encoding.UTF8.GetByteCount(text) != utf8.Length) return false;
        byte[]? rented = null;
        Span<byte> buffer = utf8.Length <= 256 ? stackalloc byte[256] : rented = ArrayPool<byte>.Shared.Rent(utf8.Length);
        try
        {
            Encoding.UTF8.GetBytes(text, buffer);
            return buffer[..utf8.Length].SequenceEqual(utf8);
        }
        finally { if (rented is not null) ArrayPool<byte>.Shared.Return(rented); }
    }

    private static ReadOnlyMemory<byte> ReadBytes(ReadOnlyMemory<byte> data, ref int position)
    {
        var length = ReadInt(data.Span, ref position);
        if (length < 0 || length > data.Length - position) throw new InvalidDataException("Truncated outbox header value.");
        var result = data.Slice(position, length);
        position += length;
        return result;
    }

    private static int ReadInt(ReadOnlySpan<byte> data, ref int position)
    {
        if (data.Length - position < 4) throw new InvalidDataException("Truncated outbox header encoding.");
        var value = BinaryPrimitives.ReadInt32LittleEndian(data[position..]);
        position += 4;
        return value;
    }
}
