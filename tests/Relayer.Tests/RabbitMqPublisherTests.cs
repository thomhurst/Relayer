using Relayer.RabbitMQ;
using RabbitMQ.Client;

namespace Relayer.Tests;

public class RabbitMqPublisherTests
{
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
