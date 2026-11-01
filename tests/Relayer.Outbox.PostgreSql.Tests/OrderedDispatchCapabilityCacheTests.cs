using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class OrderedDispatchCapabilityCacheTests
{
    [Test]
    public async Task Retention_is_bounded_and_evicted_endpoint_is_validated_again()
    {
        var cache = new OrderedDispatchCapabilityCache();
        var transport = new Transport("events");
        var transports = new Dictionary<string, ITransport> { [transport.Name] = transport };
        for (var i = 0; i < 2560; i++)
        {
            PostgreSqlOrderedDispatcher.Prepare(Row(transport.Name, i.ToString(System.Globalization.CultureInfo.InvariantCulture)), transports, cache);
            await Assert.That(cache.Count <= 256).IsTrue();
        }
        transport.Ordered = false;
        await Assert.That(() => PostgreSqlOrderedDispatcher.Prepare(Row(transport.Name, "0"), transports, cache)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Unsupported_endpoint_is_not_retained_and_lookup_can_recover()
    {
        var cache = new OrderedDispatchCapabilityCache();
        var transport = new Transport("events") { Ordered = false };
        var transports = new Dictionary<string, ITransport> { [transport.Name] = transport };
        var row = Row(transport.Name, "orders");
        await Assert.That(() => PostgreSqlOrderedDispatcher.Prepare(row, transports, cache)).Throws<InvalidOperationException>();
        transport.Ordered = true;
        PostgreSqlOrderedDispatcher.Prepare(row, transports, cache);
        await Assert.That(transport.Lookups).IsEqualTo(2);
    }

    [Test]
    public async Task Hot_endpoints_preserve_transport_and_destination_specific_capabilities()
    {
        var cache = new OrderedDispatchCapabilityCache();
        var first = new Transport("first");
        var second = new Transport("second") { Limit = 7 };
        for (var i = 0; i < 1000; i++)
        {
            await Assert.That(cache.Get(first, "orders").MaxBatchSize).IsEqualTo(1);
            await Assert.That(cache.Get(first, "other").MaxBatchSize).IsEqualTo(2);
            await Assert.That(cache.Get(second, "orders").MaxBatchSize).IsEqualTo(7);
        }
        await Assert.That(first.Lookups).IsEqualTo(2);
        await Assert.That(second.Lookups).IsEqualTo(1);
        await Assert.That(cache.Count).IsEqualTo(3);
        await Assert.That(() => cache.Get(first, "unsupported")).Throws<InvalidOperationException>();
        await Assert.That(cache.Count).IsEqualTo(3);
    }

    [Test]
    public async Task Hits_do_not_change_fifo_eviction_order()
    {
        var cache = new OrderedDispatchCapabilityCache();
        var transport = new Transport("events");
        for (var i = 0; i < OrderedDispatchCapabilityCache.Capacity; i++)
            cache.Get(transport, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        for (var i = 0; i < 1000; i++) cache.Get(transport, "0");
        cache.Get(transport, "new");
        var before = transport.Lookups;
        cache.Get(transport, "1");
        await Assert.That(transport.Lookups).IsEqualTo(before);
        cache.Get(transport, "0");
        await Assert.That(transport.Lookups).IsEqualTo(before + 1);
        await Assert.That(cache.Count).IsEqualTo(OrderedDispatchCapabilityCache.Capacity);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failed_lookup_at_capacity_preserves_entries_and_can_recover(bool throws)
    {
        var cache = new OrderedDispatchCapabilityCache();
        var transport = new Transport("events");
        for (var i = 0; i < OrderedDispatchCapabilityCache.Capacity; i++)
            cache.Get(transport, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        transport.Ordered = false;
        transport.Throws = throws;
        await Assert.That(() => cache.Get(transport, "new")).Throws<InvalidOperationException>();
        await Assert.That(() => cache.Get(transport, "new")).Throws<InvalidOperationException>();
        var before = transport.Lookups;
        cache.Get(transport, "0");
        await Assert.That(transport.Lookups).IsEqualTo(before);
        transport.Ordered = true;
        transport.Throws = false;
        cache.Get(transport, "new");
        await Assert.That(transport.Lookups).IsEqualTo(before + 1);
        cache.Get(transport, "0");
        await Assert.That(transport.Lookups).IsEqualTo(before + 2);
        await Assert.That(cache.Count).IsEqualTo(OrderedDispatchCapabilityCache.Capacity);
    }

    [Test]
    public async Task Preparation_preserves_payload_and_rejects_invalid_rows_before_lookup()
    {
        var cache = new OrderedDispatchCapabilityCache();
        var transport = new Transport("events");
        var transports = new Dictionary<string, ITransport> { [transport.Name] = transport };
        var row = Row(transport.Name, "orders");
        var prepared = PostgreSqlOrderedDispatcher.Prepare(row, transports, cache);
        await Assert.That(ReferenceEquals(prepared.Transport, transport)).IsTrue();
        await Assert.That(prepared.Message.Body.Equals(row.Message.Body)).IsTrue();
        await Assert.That(prepared.Message.Headers.Equals(row.Message.Headers)).IsTrue();
        await Assert.That(prepared.Message.OrderingKey).IsEqualTo(row.Message.OrderingKey);
        await Assert.That(prepared.Message.DeliverAt).IsNull();
        await Assert.That(() => PostgreSqlOrderedDispatcher.Prepare(Row("missing", "orders"), transports, cache)).Throws<InvalidOperationException>();
        var missingKey = new OutboxRecord(row.Id, row.TransportName, row.MessageType,
            new("orders", row.Message.Body, row.Message.Headers), row.CreatedAt, row.Lane);
        await Assert.That(() => PostgreSqlOrderedDispatcher.Prepare(missingKey, transports, cache)).Throws<InvalidOperationException>();
        await Assert.That(transport.Lookups).IsEqualTo(1);
    }

    private static OutboxRecord Row(string transport, string destination) => new(Guid.CreateVersion7(), transport, "order;v=1",
        new(destination, new byte[] { 1 }, ReadOnlyMemory<KeyValuePair<string, WireString>>.Empty, new WireString("customer"), DateTimeOffset.UtcNow), DateTimeOffset.UtcNow, 0);

    private sealed class Transport(string name) : ITransport
    {
        internal bool Ordered { get; set; } = true;
        internal int Lookups;
        internal int Limit { get; set; } = 1;
        internal bool Throws { get; set; }
        public string Name => name;
        public TransportCapabilities GetCapabilities(string endpoint)
        {
            Lookups++;
            if (Throws) throw new InvalidOperationException("Lookup failed.");
            return new() { OrderedByKey = Ordered && endpoint != "unsupported", MaxBatchSize = Limit + (endpoint == "other" ? 1 : 0) };
        }
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
