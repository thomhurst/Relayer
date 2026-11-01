using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class DirectDispatchCapabilityCacheTests
{
    [Test]
    public async Task Retention_is_bounded_across_many_destinations_and_evicted_limits_are_resolved_again()
    {
        var cache = new DirectDispatchCapabilityCache();
        var transport = new Transport("events");
        for (var i = 0; i < DirectDispatchCapabilityCache.Capacity * 10; i++)
        {
            var destination = i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            await Assert.That(cache.Get(transport, destination).MaxBatchSize).IsEqualTo(i + 1);
            await Assert.That(cache.Count <= DirectDispatchCapabilityCache.Capacity).IsTrue();
        }
        await Assert.That(cache.Count).IsEqualTo(DirectDispatchCapabilityCache.Capacity);
        var before = transport.Lookups;
        transport.LimitOffset = 1000;
        await Assert.That(cache.Get(transport, "0").MaxBatchSize).IsEqualTo(1001);
        await Assert.That(transport.Lookups).IsEqualTo(before + 1);
        await Assert.That(cache.Count).IsEqualTo(DirectDispatchCapabilityCache.Capacity);
    }

    [Test]
    public async Task Repeated_hot_destinations_use_cached_endpoint_and_transport_specific_limits()
    {
        var cache = new DirectDispatchCapabilityCache();
        var first = new Transport("first");
        var second = new Transport("second") { LimitOffset = 100 };
        for (var i = 0; i < 1000; i++)
        {
            await Assert.That(cache.Get(first, "0").MaxBatchSize).IsEqualTo(1);
            await Assert.That(cache.Get(first, "9").MaxBatchSize).IsEqualTo(10);
            await Assert.That(cache.Get(second, "0").MaxBatchSize).IsEqualTo(101);
        }
        await Assert.That(first.Lookups).IsEqualTo(2);
        await Assert.That(second.Lookups).IsEqualTo(1);
        await Assert.That(cache.Count).IsEqualTo(3);
    }

    [Test]
    public async Task Full_cache_evicts_in_insertion_order_without_changing_hot_hits()
    {
        var cache = new DirectDispatchCapabilityCache();
        var transport = new Transport("events");
        for (var i = 0; i < DirectDispatchCapabilityCache.Capacity; i++)
            cache.Get(transport, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        for (var i = 0; i < 1000; i++) cache.Get(transport, "0");
        cache.Get(transport, "1000");
        var before = transport.Lookups;
        cache.Get(transport, "1");
        await Assert.That(transport.Lookups).IsEqualTo(before);
        cache.Get(transport, "0");
        await Assert.That(transport.Lookups).IsEqualTo(before + 1);
        await Assert.That(cache.Count).IsEqualTo(DirectDispatchCapabilityCache.Capacity);
    }

    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    [Arguments(1)]
    public async Task Failed_lookup_does_not_evict_or_cache_and_can_recover(int failure)
    {
        var cache = new DirectDispatchCapabilityCache();
        var transport = new Transport("events");
        for (var i = 0; i < DirectDispatchCapabilityCache.Capacity; i++)
            cache.Get(transport, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        transport.Failure = failure;
        await Assert.That(() => cache.Get(transport, "1000")).Throws<InvalidOperationException>();
        await Assert.That(() => cache.Get(transport, "1000")).Throws<InvalidOperationException>();
        var before = transport.Lookups;
        cache.Get(transport, "0");
        await Assert.That(transport.Lookups).IsEqualTo(before);
        transport.Failure = null;
        await Assert.That(cache.Get(transport, "1000").MaxBatchSize).IsEqualTo(1001);
        await Assert.That(transport.Lookups).IsEqualTo(before + 1);
        await Assert.That(cache.Count).IsEqualTo(DirectDispatchCapabilityCache.Capacity);
    }

    private sealed class Transport(string name) : ITransport
    {
        internal int Lookups;
        internal int LimitOffset { get; set; }
        internal int? Failure { get; set; }
        public string Name => name;
        public TransportCapabilities GetCapabilities(string endpoint)
        {
            Lookups++;
            if (Failure == 1) throw new InvalidOperationException("Lookup failed.");
            return new() { MaxBatchSize = Failure ?? (int.Parse(endpoint, System.Globalization.CultureInfo.InvariantCulture) + 1 + LimitOffset) };
        }
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
