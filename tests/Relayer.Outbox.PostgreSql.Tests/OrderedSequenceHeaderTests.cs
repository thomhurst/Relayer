namespace Relayer.Outbox.PostgreSql.Tests;

public class OrderedSequenceHeaderTests
{
    [Test]
    public async Task Warmed_row_header_write_allocates_nothing()
    {
        KeyValuePair<string, WireString>[] entries = [new("ce_id", new("original"))];
        var buffer = new byte[20];
        for (var i = 0; i < 100; i++) OrderedSequenceHeader.Add(ref entries, 1, buffer, i + 1);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) OrderedSequenceHeader.Add(ref entries, 1, buffer, i + 1);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    public async Task Replaces_reserved_duplicates_and_reuses_row_storage()
    {
        KeyValuePair<string, WireString>[] entries =
            [new("ce_id", new("original")), new(OutboxHeaders.Sequence, new("fake")), new(OutboxHeaders.Sequence, new("fake"))];
        var original = entries;
        var buffer = new byte[20];
        var result = OrderedSequenceHeader.Add(ref entries, 3, buffer, long.MaxValue);
        await Assert.That(result.Length).IsEqualTo(2);
        await Assert.That(result.Span[0].Value.ToString()).IsEqualTo("original");
        await Assert.That(result.Span[1].Key).IsEqualTo(OutboxHeaders.Sequence);
        await Assert.That(result.Span[1].Value.ToString()).IsEqualTo("9223372036854775807");
        result = OrderedSequenceHeader.Add(ref entries, 2, buffer, 1);
        await Assert.That(entries).IsSameReferenceAs(original);
        await Assert.That(result.Span[1].Value.ToString()).IsEqualTo("1");
    }
}
