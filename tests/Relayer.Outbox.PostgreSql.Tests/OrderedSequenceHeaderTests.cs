using System.Buffers;

namespace Relayer.Outbox.PostgreSql.Tests;

public class OrderedSequenceHeaderTests
{
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task Warmed_row_decode_and_sequence_write_allocate_nothing(int reservedCount)
    {
        var persisted = new KeyValuePair<string, WireString>[reservedCount + 1];
        for (var i = 0; i < reservedCount; i++) persisted[i] = new(OutboxHeaders.Sequence, new("fake"));
        persisted[reservedCount] = new("ce_id", new("original"));
        var encoded = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(encoded, persisted);
        KeyValuePair<string, WireString>[] decoded = [];
        KeyValuePair<string, WireString>[] outgoing = [];
        var buffer = new byte[20];
        for (var i = 0; i < 100; i++)
        {
            var headers = OutboxHeaderCodec.Read(encoded.WrittenMemory, ref decoded);
            OrderedSequenceHeader.Add(headers.Span, ref outgoing, buffer, i + 1);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            var headers = OutboxHeaderCodec.Read(encoded.WrittenMemory, ref decoded);
            OrderedSequenceHeader.Add(headers.Span, ref outgoing, buffer, i + 1);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    public async Task Replaces_reserved_duplicates_and_reuses_row_storage()
    {
        KeyValuePair<string, WireString>[] persisted =
            [new(OutboxHeaders.Sequence, new("fake")), new("ce_id", new("original")), new(OutboxHeaders.Sequence, new("fake"))];
        KeyValuePair<string, WireString>[] entries = [];
        var buffer = new byte[20];
        var result = OrderedSequenceHeader.Add(persisted, ref entries, buffer, long.MaxValue);
        var original = entries;
        await Assert.That(result.Length).IsEqualTo(2);
        await Assert.That(result.Span[0].Value.ToString()).IsEqualTo("original");
        await Assert.That(result.Span[1].Key).IsEqualTo(OutboxHeaders.Sequence);
        await Assert.That(result.Span[1].Value.ToString()).IsEqualTo("9223372036854775807");
        await Assert.That(persisted[0].Value.ToString()).IsEqualTo("fake");
        await Assert.That(persisted[1].Key).IsEqualTo("ce_id");
        result = OrderedSequenceHeader.Add(persisted, ref entries, buffer, 1);
        await Assert.That(entries).IsSameReferenceAs(original);
        await Assert.That(result.Span[1].Value.ToString()).IsEqualTo("1");
        result = OrderedSequenceHeader.Add([], ref entries, buffer, 2);
        await Assert.That(result.Length).IsEqualTo(1);
        await Assert.That(entries[1].Key is null).IsTrue();
    }
}
