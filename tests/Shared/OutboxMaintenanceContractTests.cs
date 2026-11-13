using System.Collections.Concurrent;
using Relayer.Transports;

namespace Relayer.TestFixtures;

public abstract class OutboxMaintenanceContractTests
{
    protected abstract Task<MaintenanceFixture> CreateFixtureAsync();

    protected enum Table { Outbox, Dead, Inbox }

    protected abstract class MaintenanceFixture : IAsyncDisposable
    {
        public abstract MaintenanceWorker Worker(RecordingTransport transport, int batchSize = 1,
            bool deadRetention = true, bool inboxRetention = true, int cleanupBatchLimit = 10);
        public abstract Task Insert(Guid id, DateTimeOffset createdAt, int? lane = null,
            DateTimeOffset? deliverAt = null, string status = "pending", DateTimeOffset? claimExpiresAt = null);
        public abstract Task InsertDead(Guid id, DateTimeOffset failedAt);
        public abstract Task InsertInbox(string consumer, Guid id, DateTimeOffset processedAt);
        public abstract Task MakeDueAndExpireClaims();
        public abstract Task<int> Attempts(Guid id);
        public abstract Task<long> Count(Table table);
        public abstract ValueTask DisposeAsync();
    }

    protected abstract class MaintenanceWorker : IDisposable
    {
        public abstract ValueTask<int> SweepAsync(CancellationToken cancellationToken = default);
        public abstract bool CleanupPending { get; }
        public abstract ValueTask<int> CleanupAsync(CancellationToken cancellationToken = default);
        public abstract ValueTask ReleaseAsync(CancellationToken cancellationToken = default);
        public abstract void Dispose();
    }

    [Test]
    public async Task Recovery_respects_age_due_claim_lane_and_parked_filters_and_advances_past_failures()
    {
        await using var fixture = await CreateFixtureAsync();
        var now = DateTimeOffset.UtcNow;
        var old = now.AddDays(-1);
        await fixture.Insert(Id(1), old);
        await fixture.Insert(Id(2), old);
        await fixture.Insert(Id(3), now);
        await fixture.Insert(Id(4), old, deliverAt: now.AddDays(1));
        await fixture.Insert(Id(5), old, claimExpiresAt: now.AddDays(1));
        await fixture.Insert(Id(6), old, lane: 1);
        await fixture.Insert(Id(7), old, status: "parked");
        await fixture.Insert(Id(8), old, deliverAt: old, claimExpiresAt: now.AddSeconds(-1));
        var transport = new RecordingTransport((message, _) =>
            HeaderId(message) == Id(1) ? ValueTask.FromException(new InvalidOperationException("offline")) : ValueTask.CompletedTask);
        using var worker = fixture.Worker(transport);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(0);
        await Assert.That(transport.Ids.SequenceEqual([Id(2), Id(8)])).IsTrue();
        await Assert.That(transport.LastBody).IsEquivalentTo(new byte[] { 7, 8, 9 });
        await Assert.That(transport.LastDeliverAt).IsNull();
        await Assert.That(await fixture.Count(Table.Outbox)).IsEqualTo(6L);
        await Assert.That(await fixture.Attempts(Id(1))).IsEqualTo(1);
    }

    [Test]
    [Arguments(1)]
    [Arguments(4)]
    public async Task Recovery_revisits_failed_scheduled_and_claimed_rows_while_newer_rows_keep_arriving(int batchSize)
    {
        await using var fixture = await CreateFixtureAsync();
        var now = DateTimeOffset.UtcNow;
        var old = now.AddDays(-1);
        await fixture.Insert(Id(1), old);
        await fixture.Insert(Id(2), old, deliverAt: now.AddDays(1));
        await fixture.Insert(Id(3), old, claimExpiresAt: now.AddDays(1));
        await fixture.Insert(Id(4), old);
        var offline = true;
        var transport = new RecordingTransport((message, _) =>
            offline && HeaderId(message) == Id(1) ? ValueTask.FromException(new InvalidOperationException("offline")) : ValueTask.CompletedTask);
        using var worker = fixture.Worker(transport, batchSize);
        for (var i = 0; i < (batchSize == 1 ? 2 : 1); i++) await worker.SweepAsync();
        await Assert.That(transport.Ids.SequenceEqual([Id(4)])).IsTrue();

        offline = false;
        await fixture.MakeDueAndExpireClaims();
        for (var i = 5; i <= 10; i++)
        {
            await fixture.Insert(Id(i), old);
            await worker.SweepAsync();
        }

        for (var i = 1; i <= 5; i++) await Assert.That(transport.Ids.Contains(Id(i))).IsTrue();
        await Assert.That(transport.Ids.Distinct().Count()).IsEqualTo(transport.Ids.Count);
    }

    [Test]
    public async Task Cleanup_is_leased_bounded_and_preserves_pending_parked_and_recent_rows()
    {
        await using var fixture = await CreateFixtureAsync();
        var now = DateTimeOffset.UtcNow;
        await fixture.Insert(Id(1), now.AddDays(-60));
        await fixture.Insert(Id(2), now.AddDays(-60), status: "parked");
        await fixture.InsertDead(Id(3), now.AddDays(-60));
        await fixture.InsertDead(Id(4), now.AddDays(-60));
        await fixture.InsertDead(Id(5), now);
        await fixture.InsertInbox("a", Id(1), now.AddDays(-10));
        await fixture.InsertInbox("b", Id(1), now.AddDays(-10));
        await fixture.InsertInbox("new", Id(1), now);
        using var first = fixture.Worker(new(), cleanupBatchLimit: 1);
        using var rival = fixture.Worker(new(), cleanupBatchLimit: 1);
        await Assert.That(await first.CleanupAsync()).IsEqualTo(2);
        await Assert.That(await rival.CleanupAsync()).IsEqualTo(0);
        await Assert.That(await fixture.Count(Table.Dead)).IsEqualTo(2L);
        await Assert.That(await fixture.Count(Table.Inbox)).IsEqualTo(2L);
        await Assert.That(await fixture.Count(Table.Outbox)).IsEqualTo(2L);
        await first.ReleaseAsync();
        await Assert.That(await rival.CleanupAsync()).IsEqualTo(2);
        await Assert.That(await fixture.Count(Table.Dead)).IsEqualTo(1L);
        await Assert.That(await fixture.Count(Table.Inbox)).IsEqualTo(1L);
        await Assert.That(await fixture.Count(Table.Outbox)).IsEqualTo(2L);
        await Assert.That(await rival.CleanupAsync()).IsEqualTo(0);
    }

    [Test]
    public async Task Cleanup_drains_multiple_batches_preserves_parked_payload_and_yields_at_budget()
    {
        await using var fixture = await CreateFixtureAsync();
        var old = DateTimeOffset.UtcNow.AddDays(-60);
        await fixture.Insert(Id(90), old, status: "parked");
        await fixture.InsertDead(Id(90), old);
        for (var i = 1; i <= 7; i++)
        {
            await fixture.InsertDead(Id(i), old);
            await fixture.InsertInbox("consumer", Id(i), old);
        }
        var transport = new RecordingTransport();
        using var worker = fixture.Worker(transport, batchSize: 2, cleanupBatchLimit: 2);
        await Assert.That(await worker.CleanupAsync()).IsEqualTo(8);
        await Assert.That(worker.CleanupPending).IsTrue();
        await Assert.That(await fixture.Count(Table.Dead)).IsEqualTo(4L);
        await Assert.That(await fixture.Count(Table.Inbox)).IsEqualTo(3L);
        // A sustained backlog must yield the connection and admission before recovery.
        await fixture.Insert(Id(100), old);
        await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
        await Assert.That(transport.Ids.Contains(Id(100))).IsTrue();
        await Assert.That(await worker.CleanupAsync()).IsEqualTo(6);
        await Assert.That(worker.CleanupPending).IsFalse();
        await Assert.That(await fixture.Count(Table.Dead)).IsEqualTo(1L);
        await Assert.That(await fixture.Count(Table.Inbox)).IsEqualTo(0L);
        await Assert.That(await fixture.Count(Table.Outbox)).IsEqualTo(1L);
    }

    [Test]
    public async Task Cancelled_cleanup_leaves_rows_and_admission_available_for_retry()
    {
        await using var fixture = await CreateFixtureAsync();
        var old = DateTimeOffset.UtcNow.AddDays(-60);
        for (var i = 1; i <= 3; i++)
        {
            await fixture.InsertDead(Id(i), old);
            await fixture.InsertInbox("consumer", Id(i), old);
        }
        using var worker = fixture.Worker(new());
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await worker.CleanupAsync(cancelled.Token));
        await Assert.That(await fixture.Count(Table.Dead)).IsEqualTo(3L);
        await Assert.That(await fixture.Count(Table.Inbox)).IsEqualTo(3L);
        await Assert.That(await worker.CleanupAsync()).IsEqualTo(6);
        await Assert.That(worker.CleanupPending).IsFalse();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    public async Task Cleanup_can_disable_each_retention_independently(bool deadRetention, bool inboxRetention)
    {
        await using var fixture = await CreateFixtureAsync();
        var old = DateTimeOffset.UtcNow.AddDays(-60);
        await fixture.InsertDead(Id(1), old);
        await fixture.InsertInbox("a", Id(1), old);
        using var worker = fixture.Worker(new(), deadRetention: deadRetention, inboxRetention: inboxRetention);
        await Assert.That(await worker.CleanupAsync()).IsEqualTo((deadRetention ? 1 : 0) + (inboxRetention ? 1 : 0));
        await Assert.That(await fixture.Count(Table.Dead)).IsEqualTo(deadRetention ? 0L : 1L);
        await Assert.That(await fixture.Count(Table.Inbox)).IsEqualTo(inboxRetention ? 0L : 1L);
    }

    private static Guid Id(int i) => Guid.Parse($"01900000-0000-7000-8000-{i:D12}");
    private static Guid HeaderId(OutgoingMessage message) => Guid.Parse(message.Headers.Span[0].Value.ToString());

    protected sealed class RecordingTransport(Func<OutgoingMessage, CancellationToken, ValueTask>? send = null) : ITransport
    {
        public string Name => "events";
        public ConcurrentQueue<Guid> Ids { get; } = new();
        public byte[]? LastBody { get; private set; }
        public DateTimeOffset? LastDeliverAt { get; private set; }
        public TransportCapabilities GetCapabilities(string endpoint) => new() { MaxBatchSize = 1 };
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            if (send is not null) await send(message, cancellationToken);
            Ids.Enqueue(HeaderId(message));
            LastBody = message.Body.ToArray();
            LastDeliverAt = message.DeliverAt;
        }
        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], cancellationToken);
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
