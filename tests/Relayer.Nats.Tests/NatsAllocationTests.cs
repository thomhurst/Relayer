using System.Buffers;
using System.Text;
using NATS.Client.Core;

namespace Relayer.Nats.Tests;

public class NatsAllocationTests
{
    [Test]
    [Arguments("Nats-Msg-Id")]
    [Arguments("Nats-Rollup")]
    [Arguments("nats-rollup")]
    [Arguments("Nats-Expected-Stream")]
    [Arguments("Nats-Expected-Last-Msg-Id")]
    [Arguments("Nats-Expected-Last-Sequence")]
    [Arguments("Nats-Expected-Last-Subject-Sequence")]
    [Arguments("Nats-Expected-Last-Subject-Sequence-Subject")]
    public async Task Failure_copy_clears_publish_options_and_reset_restores_normal_sends(string name)
    {
        var headers = new NatsPublishHeaders(false) { IsFailureCopy = true };
        var context = new NatsMsgContext("dead", headers: headers);
        var writer = new ArrayBufferWriter<byte>();
        // Native PublishAsync applies NatsJSPubOpts before invoking this serializer hook.
        headers[name] = "configured-value";
        headers.Serialize(writer, ReadOnlyMemory<byte>.Empty, context);
        await Assert.That(headers.ContainsKey(name)).IsFalse();
        headers.Reset();
        headers[name] = "ordinary-send";
        headers.Serialize(writer, ReadOnlyMemory<byte>.Empty, context);
        await Assert.That(headers[name].ToString()).IsEqualTo("ordinary-send");
    }

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
        await Assert.That(headers["traceparent"].ToString()).IsEqualTo(headers.TraceParent.ToString());
        await Assert.That(headers["tracestate"].ToString()).IsEqualTo(headers.TraceState.ToString());
        await Assert.That(writer.WrittenSpan.SequenceEqual(body)).IsTrue();
    }
}
