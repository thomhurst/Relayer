using Relayer.Transports;
using Relayer.RabbitMQ;
using RabbitMQ.Client;

namespace Relayer.Tests;

public class RabbitMqPublisherTests
{
    [Test]
    public async Task Batch_slots_do_not_reserve_full_boundary_caches()
    {
        var publisher = new RabbitMqPublisher(null!);
        publisher.BatchSlot(0);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 1; i <= 1000; i++) publisher.BatchSlot(i);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        // Allows small slot state and list growth, but rejects 256-entry caches per slot.
        await Assert.That(allocated < 2_000_000L).IsTrue();
        GC.KeepAlive(publisher);
    }

    [Test]
    public async Task Metrics_endpoint_is_created_without_message_tracing()
    {
        using var instrumentation = new RelayerInstrumentation();
        var endpoint = new RabbitMqSendEndpoint("orders", new ConnectionFactory(), null, instrumentation,
            static (_, _) => ValueTask.CompletedTask, static (_, _) => ValueTask.CompletedTask);
        await Assert.That(endpoint.Telemetry).IsNotNull();
        await Assert.That(endpoint.Tracing).IsNull();
    }

    [Test]
    public async Task Header_cache_evicts_FIFO_without_reusing_buffers_still_in_a_message()
    {
        var publisher = new RabbitMqPublisher(null!);
        var headers = new KeyValuePair<string, WireString>[RabbitMqPublisher.CacheCapacity + 1];
        for (var i = 0; i < headers.Length; i++)
            headers[i] = new($"header-{i}", new(new byte[] { 0, (byte)i, 0 }.AsMemory(1, 1)));
        var message = new OutgoingMessage("orders", default, headers);
        publisher.Prepare(message, 0);
        var first = (byte[])publisher.Properties.Headers!["header-0"]!;
        var last = (byte[])publisher.Properties.Headers["header-256"]!;
        await Assert.That(ReferenceEquals(first, last)).IsFalse();
        await Assert.That(first[0]).IsEqualTo((byte)0);
        publisher.Clear();
        publisher.Prepare(new("orders", default, headers.AsMemory(0, 1)), 0);
        await Assert.That(ReferenceEquals(first, publisher.Properties.Headers!["header-0"])).IsFalse();
    }

    [Test]
    public async Task Header_miss_allocates_only_the_exact_array_required_by_native_client()
    {
        var publisher = new RabbitMqPublisher(null!);
        var headers = new KeyValuePair<string, WireString>[] { new("id", new(new byte[3].AsMemory(1, 1))) };
        for (var i = 1; i <= RabbitMqPublisher.CacheCapacity; i++)
        {
            headers[0] = new("id", new(new byte[i + 2].AsMemory(1, i)));
            publisher.Prepare(new("orders", default, headers), 0);
            publisher.Clear();
        }
        var value = new WireString(new byte[1026].AsMemory(1, 1024));
        headers[0] = new("id", value);
        var message = new OutgoingMessage("orders", default, headers);
        var before = GC.GetAllocatedBytesForCurrentThread();
        publisher.Prepare(message, 0);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        before = GC.GetAllocatedBytesForCurrentThread();
        var raw = value.Utf8.ToArray();
        var nativeAllocated = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(raw);
        await Assert.That(allocated).IsEqualTo(nativeAllocated);
        await Assert.That(((byte[])publisher.Properties.Headers!["id"]!).Length).IsEqualTo(1024);
    }

    [Test]
    public async Task Content_type_cache_evicts_oldest_and_decodes_new_values()
    {
        var publisher = new RabbitMqPublisher(null!);
        var headers = new KeyValuePair<string, WireString>[1];
        string? first = null;
        for (var i = 0; i <= RabbitMqPublisher.CacheCapacity; i++)
        {
            headers[0] = new("content-type", new(System.Text.Encoding.UTF8.GetBytes($"application/type-{i}")));
            publisher.Prepare(new("orders", default, headers), 0);
            if (i == 0) first = publisher.Properties.ContentType;
            await Assert.That(publisher.Properties.ContentType).IsEqualTo($"application/type-{i}");
            publisher.Clear();
        }
        headers[0] = new("content-type", new("application/type-0"u8.ToArray()));
        publisher.Prepare(new("orders", default, headers), 0);
        await Assert.That(publisher.Properties.ContentType).IsEqualTo(first);
        await Assert.That(ReferenceEquals(first, publisher.Properties.ContentType)).IsFalse();
    }

    [Test]
    public async Task Verified_plugin_accepts_native_timer_upper_bound_and_rejects_larger_delays()
    {
        var endpoint = new RabbitMqSendEndpoint("orders", new ConnectionFactory(), null, null,
            static (_, _) => ValueTask.CompletedTask, static (_, _) => ValueTask.CompletedTask)
        { DelayedExchange = "verified.delayed" };
        // The plugin accepts positive delays strictly below 2^32 milliseconds.
        var maximum = endpoint.Delay(DateTimeOffset.UtcNow.AddMilliseconds(uint.MaxValue));
        await Assert.That(maximum <= uint.MaxValue && maximum > uint.MaxValue - 1000L).IsTrue();
        await Assert.That(() => endpoint.Delay(DateTimeOffset.UtcNow.AddMilliseconds((long)uint.MaxValue + 60_000)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task Preparing_alternating_utf8_shapes_content_types_and_delays_allocates_nothing_after_warmup()
    {
        var publisher = new RabbitMqPublisher(null!);
        var first = new OutgoingMessage("orders", default, new KeyValuePair<string, WireString>[]
        {
            new("cloudEvents:id", new("__first__"u8.ToArray().AsMemory(2, 5))),
            new("content-type", new("application/json"u8.ToArray())),
        });
        var second = new OutgoingMessage("orders", default, new KeyValuePair<string, WireString>[]
        {
            new("cloudEvents:id", new("__second__"u8.ToArray().AsMemory(2, 6))),
            new("content-type", new("application/octet-stream"u8.ToArray())),
        });
        for (var i = 0; i < 1000; i++)
        {
            publisher.Prepare(i % 2 == 0 ? first : second, i);
            publisher.Clear();
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            publisher.Prepare(i % 2 == 0 ? first : second, i);
            publisher.Clear();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }
}
