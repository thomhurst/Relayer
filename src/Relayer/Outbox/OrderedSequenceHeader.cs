using Relayer.Transports;
using System.Buffers.Text;

namespace Relayer;

// Storage belongs to the reader's reusable row slot and remains borrowed until its next read.
internal static class OrderedSequenceHeader
{
    internal static ReadOnlyMemory<KeyValuePair<string, WireString>> Add(
        ReadOnlySpan<KeyValuePair<string, WireString>> persisted,
        ref KeyValuePair<string, WireString>[] entries, byte[] buffer, long sequence)
    {
        if (sequence <= 0) throw new InvalidDataException("Ordered outbox insert sequence must be positive.");
        Utf8Formatter.TryFormat(sequence, buffer, out var written);
        if (entries.Length < persisted.Length + 1) Array.Resize(ref entries, persisted.Length + 1);
        var target = 0;
        // Keep the persisted decoder slots unchanged so their header-name cache survives the next read.
        // Replace producer-supplied copies of the reserved header with the database value.
        for (var i = 0; i < persisted.Length; i++)
            if (!StringComparer.Ordinal.Equals(persisted[i].Key, OutboxHeaders.Sequence)) entries[target++] = persisted[i];
        entries[target++] = new(OutboxHeaders.Sequence, new WireString(buffer.AsMemory(0, written)));
        entries.AsSpan(target).Clear();
        return entries.AsMemory(0, target);
    }
}
