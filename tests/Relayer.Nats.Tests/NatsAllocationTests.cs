using System.Buffers;
using System.Text;
using NATS.Client.Core;

namespace Relayer.Nats.Tests;

public class NatsAllocationTests
{
    [Test]
    public async Task Utf8_header_conversion_matches_the_native_string_requirement()
    {
        var utf8 = "changing-event-identifier"u8.ToArray();
        var value = new WireString(utf8);
        var headers = new NatsHeaders(1);
        for (var i = 0; i < 1000; i++) { headers["ce-id"] = value.ToString(); headers["ce-id"] = Encoding.UTF8.GetString(utf8); }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) headers["ce-id"] = Encoding.UTF8.GetString(utf8);
        var native = GC.GetAllocatedBytesForCurrentThread() - before;
        before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) headers["ce-id"] = value.ToString();
        var relayer = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(native).IsGreaterThan(0L);
        await Assert.That(relayer).IsEqualTo(native);
    }

    [Test]
    public async Task Native_context_hook_preserves_creation_headers_without_adding_allocations()
    {
        var headers = new NatsPublishHeaders(false) { TraceParent = "stored-traceparent", TraceState = "stored-tracestate" };
        var context = new NatsMsgContext("orders", headers: headers);
        var body = "payload"u8.ToArray();
        var value = new ReadOnlyMemory<byte>(body);
        var writer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < 1000; i++)
        {
            writer.Clear();
            headers.Serialize(writer, value, context);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            writer.Clear();
            headers.Serialize(writer, value, context);
        }
        var added = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(added).IsEqualTo(0L);
        await Assert.That(headers["traceparent"].ToString()).IsEqualTo(headers.TraceParent);
        await Assert.That(headers["tracestate"].ToString()).IsEqualTo(headers.TraceState);
        await Assert.That(writer.WrittenSpan.SequenceEqual(body)).IsTrue();
    }
}
