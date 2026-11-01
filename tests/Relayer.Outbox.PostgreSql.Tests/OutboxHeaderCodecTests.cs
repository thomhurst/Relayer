using System.Buffers;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.PostgreSql.Tests;

public class OutboxHeaderCodecTests
{
    [Test]
    public async Task Warm_codec_preserves_names_and_bytes_without_allocating()
    {
        KeyValuePair<string, WireString>[] headers = [new("type", new("order.v1")), new(new string('é', 300), new(new byte[] { 255, 0, 254 })), new("empty", new(""))];
        var buffer = new ArrayBufferWriter<byte>(2048);
        KeyValuePair<string, WireString>[] entries = [];
        for (var i = 0; i < 100; i++)
        {
            buffer.Clear();
            OutboxHeaderCodec.Write(buffer, headers);
            OutboxHeaderCodec.Read(buffer.WrittenMemory, ref entries);
        }
        var decodedName = entries[1].Key;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++)
        {
            buffer.Clear();
            OutboxHeaderCodec.Write(buffer, headers);
            OutboxHeaderCodec.Read(buffer.WrittenMemory, ref entries);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(entries[1].Key).IsSameReferenceAs(decodedName);
        await Assert.That(entries[1].Key).IsEqualTo(headers[1].Key);
        await Assert.That(entries[1].Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 255, 0, 254 });
    }

    [Test]
    public async Task Corrupt_encodings_fail_clearly()
    {
        foreach (var data in new byte[][] { [], [1], [2, 0, 0, 0, 0, 0, 0, 0],
            [1, 0, 0, 0, 255, 255, 255, 255], [1, 0, 0, 0, 1, 0, 0, 0],
            [1, 0, 0, 0, 0, 0, 0, 0, 1],
            [1, 0, 0, 0, 1, 0, 0, 0, 50, 0, 0, 0, 0, 0, 0, 0] })
        {
            await Assert.ThrowsAsync<InvalidDataException>(() =>
            {
                KeyValuePair<string, WireString>[] entries = [];
                OutboxHeaderCodec.Read(data, ref entries);
                return Task.CompletedTask;
            });
        }
    }
}
