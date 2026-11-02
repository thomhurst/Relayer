using System.Buffers;

namespace Relayer.Outbox.PostgreSql;

// Retain the provider's internal entry point while sharing its wire format with other durable stores.
internal static class OutboxHeaderCodec
{
    internal static void Write(IBufferWriter<byte> writer, ReadOnlySpan<KeyValuePair<string, WireString>> headers) =>
        DurableOutboxHeaderCodec.Write(writer, headers);

    internal static ReadOnlyMemory<KeyValuePair<string, WireString>> Read(ReadOnlyMemory<byte> data,
        ref KeyValuePair<string, WireString>[] entries) => DurableOutboxHeaderCodec.Read(data, ref entries);
}
