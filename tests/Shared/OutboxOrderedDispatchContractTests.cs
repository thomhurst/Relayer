using System.Collections.Concurrent;
using Relayer.Transports;

namespace Relayer.TestFixtures;

public abstract class OutboxOrderedDispatchContractTests
{
    protected abstract Task<OrderedFixture> CreateFixtureAsync();

    public enum HeadState { Future, Claimed, Parked }
    protected sealed record Row(Guid Id, int Attempts, string? Error, bool HasClaimToken, bool HasClaimExpiry);

    protected abstract class OrderedFixture : IAsyncDisposable
    {
        public abstract OrderedWorker Worker(RecordingTransport transport);
        public abstract Task Insert(Guid id, int? lane, string orderingKey = "customer-Ã©");
        public abstract Task SetRouting(Guid id, string transport = "events", string? orderingKey = "customer-Ã©", string destination = "orders");
        public abstract Task SetHeadState(Guid id, HeadState state);
        public abstract Task MakeReady(Guid id);
        public abstract Task<Row[]> ReadRows();
        public abstract ValueTask DisposeAsync();
    }

    protected abstract class OrderedWorker : IDisposable
    {
        public abstract ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken = default);
        public abstract void Dispose();
    }

    [Test]
    public async Task Failed_head_blocks_later_rows_and_retries_preserve_native_payload()
    {
        await using var fixture = await CreateFixtureAsync();
        // Each insert commits before the next: database insertion order wins over host UUID order.
        await fixture.Insert(Id(2), 0);
        await fixture.Insert(Id(1), 0);
        await fixture.Insert(Id(3), null);
        var fail = true;
        var transport = new RecordingTransport((_, _) => fail
            ? ValueTask.FromException(new InvalidOperationException("broker offline")) : ValueTask.CompletedTask);
        using var worker = fixture.Worker(transport);
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(0);
        await Assert.That(transport.Ids.Count).IsEqualTo(0);
        var rows = await fixture.ReadRows();
        await Assert.That(rows.Length).IsEqualTo(3);
        await Assert.That(rows[0]).IsEqualTo(new Row(Id(1), 0, null, false, false));
        await Assert.That(rows[1]).IsEqualTo(new Row(Id(2), 1, "broker offline", false, false));
        fail = false;
        await fixture.MakeReady(Id(2)); // Retry eligibility may include a durable backoff.
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(2);
        await Assert.That(transport.Ids.SequenceEqual([Id(2), Id(1)])).IsTrue();
        await Assert.That(transport.LastBody).IsEquivalentTo(new byte[] { 7, 8, 9 });
        await Assert.That(transport.LastKey).IsEqualTo("customer-Ã©");
        await Assert.That(transport.LastDeliverAt).IsNull();
        await Assert.That(await fixture.ReadRows()).IsEquivalentTo(new[] { new Row(Id(3), 0, null, false, false) });
    }

    [Test]
    [Arguments("unknown_transport")]
    [Arguments("missing_key")]
    [Arguments("unsupported_endpoint")]
    public async Task Preparation_failure_commits_successful_prefix_and_stops_before_later_rows(string kind)
    {
        await using var fixture = await CreateFixtureAsync();
        for (var index = 1; index <= 3; index++) await fixture.Insert(Id(index), 0);
        switch (kind)
        {
            case "unknown_transport": await fixture.SetRouting(Id(2), transport: "missing"); break;
            case "missing_key": await fixture.SetRouting(Id(2), orderingKey: null); break;
            case "unsupported_endpoint": await fixture.SetRouting(Id(2), destination: "unsupported"); break;
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }
        var transport = new RecordingTransport();
        using var worker = fixture.Worker(transport);
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(1);
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(0);
        await Assert.That(transport.Ids.SequenceEqual([Id(1)])).IsTrue();
        var rows = await fixture.ReadRows();
        await Assert.That(rows.Length).IsEqualTo(2);
        await Assert.That(rows[0].Id).IsEqualTo(Id(2));
        await Assert.That(rows[0].Attempts).IsEqualTo(2);
        await Assert.That(string.IsNullOrEmpty(rows[0].Error)).IsFalse();
        await Assert.That(rows[0].HasClaimToken || rows[0].HasClaimExpiry).IsFalse();
        await Assert.That(rows[1]).IsEqualTo(new Row(Id(3), 0, null, false, false));
        await fixture.SetRouting(Id(2));
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(2);
        await Assert.That(transport.Ids.SequenceEqual([Id(1), Id(2), Id(3)])).IsTrue();
        await Assert.That((await fixture.ReadRows()).Length).IsEqualTo(0);
    }

    [Test]
    [Arguments(HeadState.Future)]
    [Arguments(HeadState.Claimed)]
    [Arguments(HeadState.Parked)]
    public async Task Ineligible_head_is_never_skipped(HeadState state)
    {
        await using var fixture = await CreateFixtureAsync();
        await fixture.Insert(Id(1), 0);
        await fixture.Insert(Id(2), 0);
        await fixture.SetHeadState(Id(1), state);
        var transport = new RecordingTransport();
        using var worker = fixture.Worker(transport);
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(0);
        await Assert.That(transport.Ids.Count).IsEqualTo(0);
        var rows = await fixture.ReadRows();
        await Assert.That(rows.Length).IsEqualTo(2);
        await Assert.That(rows[1]).IsEqualTo(new Row(Id(2), 0, null, false, false));
        await fixture.MakeReady(Id(1));
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(2);
        await Assert.That(transport.Ids.SequenceEqual([Id(1), Id(2)])).IsTrue();
        await Assert.That((await fixture.ReadRows()).Length).IsEqualTo(0);
    }

    [Test]
    public async Task Parked_keys_do_not_block_other_keys_in_the_same_lane()
    {
        await using var fixture = await CreateFixtureAsync();
        await fixture.Insert(Id(1), 0);
        await fixture.Insert(Id(2), 0);
        await fixture.Insert(Id(3), 0, "other-key");
        await fixture.SetHeadState(Id(1), HeadState.Parked);
        var transport = new RecordingTransport();
        using var worker = fixture.Worker(transport);
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(1);
        await Assert.That(transport.Ids.SequenceEqual([Id(3)])).IsTrue();
        await Assert.That((await fixture.ReadRows()).Select(row => row.Id)).IsEquivalentTo(new[] { Id(1), Id(2) });
        await fixture.MakeReady(Id(1));
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(2);
        await Assert.That(transport.Ids.SequenceEqual([Id(3), Id(1), Id(2)])).IsTrue();
        await Assert.That((await fixture.ReadRows()).Length).IsEqualTo(0);
    }

    [Test]
    public async Task Unsupported_native_ordering_keeps_rows_durable_without_sending()
    {
        await using var fixture = await CreateFixtureAsync();
        await fixture.Insert(Id(1), 0);
        await fixture.Insert(Id(2), 0);
        var transport = new RecordingTransport { Ordered = false };
        using var worker = fixture.Worker(transport);
        await Assert.That(await worker.DispatchLaneAsync(0)).IsEqualTo(0);
        await Assert.That(transport.Ids.Count).IsEqualTo(0);
        var rows = await fixture.ReadRows();
        await Assert.That(rows.Length).IsEqualTo(2);
        await Assert.That(rows[0].Id).IsEqualTo(Id(1));
        await Assert.That(rows[0].Attempts).IsEqualTo(1);
        await Assert.That(string.IsNullOrEmpty(rows[0].Error)).IsFalse();
        await Assert.That(rows[0].HasClaimToken || rows[0].HasClaimExpiry).IsFalse();
        await Assert.That(rows[1]).IsEqualTo(new Row(Id(2), 0, null, false, false));
    }

    private static Guid Id(int number) => Guid.Parse($"01900000-0000-7000-8000-{number:x12}");

    protected sealed class RecordingTransport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        public string Name => "events";
        public ConcurrentQueue<Guid> Ids { get; } = new();
        public byte[]? LastBody { get; private set; }
        public string? LastKey { get; private set; }
        public DateTimeOffset? LastDeliverAt { get; private set; }
        public bool Ordered { get; init; } = true;
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = Ordered && endpoint != "unsupported" };
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null) await send(message, cancellationToken);
            Ids.Enqueue(Guid.Parse(message.Headers.Span[0].Value.ToString()));
            LastBody = message.Body.ToArray();
            LastKey = message.OrderingKey?.ToString();
            LastDeliverAt = message.DeliverAt;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
