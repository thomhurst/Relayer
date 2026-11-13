using Relayer.Transports;
using System.Buffers;
using System.Text;
using Dekaf.Serialization;

namespace Relayer.Kafka.Tests;

public class KafkaSendTests
{
    [Test]
    public async Task Record_transforms_compose_with_headers_reject_outbox_and_clear_pooled_state()
    {
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(123456789);
        var options = new SendOptions().Kafka(static h => h.Add("before", "yes"))
            .KafkaRecord(record => record with { Partition = 2, Timestamp = timestamp })
            .Kafka(static h => h.Add("after", "yes"));
        var outgoing = new OutgoingMessage("orders", new byte[] { 42 }, orderingKey: new("key"), options: options);
        var state = new KafkaSendState();
        state.Prepare(outgoing);
        await Assert.That(state.Record!.Partition).IsEqualTo(2);
        await Assert.That(state.Record.Timestamp).IsEqualTo(timestamp);
        await Assert.That(state.Record.Headers!.GetFirstAsString("before")).IsEqualTo("yes");
        await Assert.That(state.Record.Headers.GetFirstAsString("after")).IsEqualTo("yes");
        await Assert.That(() => outgoing.ValidateForOutbox()).Throws<InvalidOperationException>().WithMessageContaining("Native configurators");
        var buffered = new SendOptions();
        outgoing.CopyNativeOptionsTo(buffered);
        state.Prepare(new("orders", default, options: buffered));
        await Assert.That(state.Record!.Partition).IsEqualTo(2);
        buffered.Reset();
        state.Prepare(new("orders", default, options: buffered));
        await Assert.That(state.Record).IsNull();
        state.Prepare(new("orders", default));
        await Assert.That(state.Record).IsNull();
        await Assert.That(state.Headers.Count).IsEqualTo(0);
        var invalid = new SendOptions().KafkaRecord(static record => record with { Topic = "other" });
        await Assert.That(() => state.Prepare(new("orders", default, options: invalid)))
            .Throws<InvalidOperationException>().WithMessageContaining("destination topic");
        var nullRecord = new SendOptions().KafkaRecord(static _ => null!);
        await Assert.That(() => state.Prepare(new("orders", default, options: nullRecord)))
            .Throws<InvalidOperationException>().WithMessageContaining("returned null");
    }

    [Test]
    public async Task Warm_operation_pool_retains_more_than_64_concurrent_sends_without_allocating()
    {
        var pool = new KafkaSendPool();
        var operations = new KafkaSendOperation[128];
        for (var round = 0; round < 2; round++)
        {
            for (var i = 0; i < operations.Length; i++) operations[i] = pool.Rent(1);
            for (var i = 0; i < operations.Length; i++) pool.Return(operations[i]);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < operations.Length; i++) operations[i] = pool.Rent(1);
        for (var i = 0; i < operations.Length; i++) pool.Return(operations[i]);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    public async Task Serialization_preserves_native_headers_and_only_replaces_existing_creation_context()
    {
        var serializer = new KafkaValueSerializer();
        var output = new ArrayBufferWriter<byte>();
        var nativeHeaders = new Headers().Add("interceptor", "value").Add("traceparent", "native-parent").Add("tracestate", "native=state");
        var original = new Headers().Add("traceparent", "creation-parent").Add("duplicate", "first").Add("duplicate", "second");
        var context = new SerializationContext { Headers = nativeHeaders };
        serializer.Serialize((ReadOnlyMemory<byte>.Empty, original), ref output, context);
        await Assert.That(nativeHeaders.GetFirstAsString("interceptor")).IsEqualTo("value");
        await Assert.That(nativeHeaders.GetFirstAsString("traceparent")).IsEqualTo("creation-parent");
        await Assert.That(nativeHeaders.GetFirstAsString("tracestate")).IsNull();
        var duplicates = nativeHeaders.GetAll("duplicate").ToArray();
        await Assert.That(duplicates.Length).IsEqualTo(2);
        await Assert.That(duplicates[0].GetValueAsString()).IsEqualTo("first");
        await Assert.That(duplicates[1].GetValueAsString()).IsEqualTo("second");
        var nativeOnly = new Headers().Add("traceparent", "native-only");
        serializer.Serialize((ReadOnlyMemory<byte>.Empty, new Headers()), ref output, new() { Headers = nativeOnly });
        await Assert.That(nativeOnly.GetFirstAsString("traceparent")).IsEqualTo("native-only");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Serialization_preserves_duplicate_order_null_values_and_body(bool populated)
    {
        var serializer = new KafkaValueSerializer();
        var output = new ArrayBufferWriter<byte>();
        var native = new Headers();
        if (populated)
            native.Add("duplicate", "old-first").Add("keep", "native").Add("duplicate", "old-last")
                .Add("traceparent", "native-parent").Add("tracestate", "native=state");
        var application = new Headers().Add("duplicate", "first").Add("traceparent", "creation-parent")
            .Add("duplicate", (byte[]?)null).Add("duplicate", "last");
        byte[] body = [1, 2, 3];

        serializer.Serialize((body, application), ref output, new() { Headers = native });

        await Assert.That(output.WrittenMemory.ToArray()).IsEquivalentTo(body);
        var offset = populated ? 1 : 0;
        await Assert.That(native.Count).IsEqualTo(offset + application.Count);
        if (populated) await Assert.That(native[0].Key).IsEqualTo("keep");
        for (var i = 0; i < application.Count; i++)
            await Assert.That(native[offset + i]).IsEqualTo(application[i]);
        await Assert.That(native[offset + 2].IsValueNull).IsTrue();
        await Assert.That(native.GetFirstAsString("tracestate")).IsNull();
    }

    [Test]
    public async Task Borrows_utf8_memory_encodes_text_and_clears_native_headers_between_sends()
    {
        var state = new KafkaSendState();
        byte[] bytes = [1, 2, 3];
        KeyValuePair<string, WireString>[] headers = [new("bytes", new(bytes)), new("text", new("héllo"))];
        var options = new SendOptions().Kafka(static native => native.Add("custom", "yes"));
        state.Prepare(new("orders", bytes, headers, new WireString(bytes), options: options));
        await Assert.That(state.Key!.Value.Utf8.Equals(bytes.AsMemory())).IsTrue();
        await Assert.That(state.Headers[0].Value.Equals(bytes.AsMemory())).IsTrue();
        await Assert.That(state.Headers.GetFirstAsString("text")).IsEqualTo("héllo");
        await Assert.That(state.Headers.GetFirstAsString("custom")).IsEqualTo("yes");
        state.Prepare(new("orders", bytes, orderingKey: new WireString("customer-1")));
        await Assert.That(state.Key!.Value.ToString()).IsEqualTo("customer-1");
        await Assert.That(state.Headers.Count).IsEqualTo(0);
        state.Clear();
        await Assert.That(state.Key).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Warm_send_preparation_and_native_serialization_allocate_zero_bytes(bool populated)
    {
        var state = new KafkaSendState();
        var serializer = new KafkaValueSerializer();
        var keySerializer = new KafkaKeySerializer();
        var output = new ArrayBufferWriter<byte>(256);
        var nativeHeaders = new Headers(16);
        KeyValuePair<string, WireString>[] headers = [new("ce_type", new("tests.order;v=1")), new("tenant", new("tenant-1"))];
        var message = new OutgoingMessage("orders", new byte[32], headers, new WireString("customer-1"));
        var context = new SerializationContext { Headers = nativeHeaders };
        for (var i = 0; i < 1000; i++)
        {
            output.Clear();
            if (!populated) nativeHeaders.Clear();
            state.Prepare(message);
            keySerializer.Serialize(state.Key, ref output, context);
            serializer.Serialize((message.Body, state.Headers), ref output, context);
            state.Clear();
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            output.Clear();
            if (!populated) nativeHeaders.Clear();
            state.Prepare(message);
            keySerializer.Serialize(state.Key, ref output, context);
            serializer.Serialize((message.Body, state.Headers), ref output, context);
            state.Clear();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(nativeHeaders.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Native_callback_errors_and_mismatches_do_not_leak_prepared_headers()
    {
        var state = new KafkaSendState();
        var options = new SendOptions().Kafka(static headers =>
        {
            headers.Add("failed", "value");
            throw new InvalidOperationException("callback failure");
        });
        await Assert.That(() => state.Prepare(new("orders", default, options: options))).Throws<InvalidOperationException>();
        state.Prepare(new("orders", default));
        await Assert.That(state.Headers.Count).IsEqualTo(0);
        var mismatch = new SendOptions().ConfigureNative<StringBuilder>(static _ => { });
        await Assert.That(() => state.Prepare(new("orders", default, options: mismatch)))
            .Throws<InvalidOperationException>().WithMessageContaining("native outgoing type");
    }
}
