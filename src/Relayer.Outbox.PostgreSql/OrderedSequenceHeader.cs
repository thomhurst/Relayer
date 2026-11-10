using System.Buffers.Text;

namespace Relayer.Outbox.PostgreSql;

// Storage belongs to the reader's reusable row slot and remains borrowed until its next read.
internal static class OrderedSequenceHeader
{
    internal static ReadOnlyMemory<KeyValuePair<string, WireString>> Add(
        ref KeyValuePair<string, WireString>[] entries, int count, byte[] buffer, long sequence)
    {
        if (sequence <= 0) throw new InvalidDataException("Ordered outbox insert sequence must be positive.");
        Utf8Formatter.TryFormat(sequence, buffer, out var written);
        var target = 0;
        // Replace producer-supplied copies of the reserved header with the database value.
        for (var i = 0; i < count; i++)
            if (!StringComparer.Ordinal.Equals(entries[i].Key, OutboxHeaders.Sequence)) entries[target++] = entries[i];
        if (entries.Length <= target) Array.Resize(ref entries, target + 1);
        entries[target++] = new(OutboxHeaders.Sequence, new WireString(buffer.AsMemory(0, written)));
        entries.AsSpan(target, count > target ? count - target : 0).Clear();
        return entries.AsMemory(0, target);
    }
}
