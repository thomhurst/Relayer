using Relayer.Transports;
namespace Relayer.TestFixtures;

// Shared durable-store behaviours. Providers own setup and seeding; tests use only the portable contract.
public abstract class OutboxStoreContractTests
{
    protected abstract Task<StoreFixture> CreateFixtureAsync();

    protected abstract class StoreFixture : IAsyncDisposable
    {
        public abstract IOutboxStore Store();
        public abstract Task Insert(Guid id, DateTimeOffset now, int? lane = null, DateTimeOffset? deliverAt = null,
            string status = "pending", DateTimeOffset? expires = null);
        public abstract ValueTask DisposeAsync();
    }

    [Test]
    public async Task Bounded_reads_filter_lanes_due_time_status_and_live_claims()
    {
        await using var fixture = await CreateFixtureAsync();
        var store = fixture.Store();
        var now = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var first = Guid.Parse("01900000-0000-7000-8000-000000000001");
        var second = Guid.Parse("01900000-0000-7000-8000-000000000002");
        var third = Guid.Parse("01900000-0000-7000-8000-000000000003");
        await fixture.Insert(third, now);
        await fixture.Insert(first, now, deliverAt: now);
        await fixture.Insert(second, now);
        await fixture.Insert(Guid.CreateVersion7(), now, lane: 2);
        await fixture.Insert(Guid.CreateVersion7(), now, deliverAt: now.AddSeconds(1));
        await fixture.Insert(Guid.CreateVersion7(), now, status: "parked");
        await fixture.Insert(Guid.CreateVersion7(), now, expires: now.AddSeconds(1));
        await Assert.That(await store.GetPendingCountAsync(now, default)).IsEqualTo(4L);
        var rows = new OutboxRecord[2];
        await Assert.That(await store.ReadPendingAsync(new(null, now), rows, default)).IsEqualTo(2);
        await Assert.That(rows[0].Id).IsEqualTo(first);
        await Assert.That(rows[1].Id).IsEqualTo(second);
        await Assert.That(await store.ReadPendingAsync(new(null, now, second), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(third);
        await Assert.That(await store.ReadPendingAsync(new(2, now), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Lane).IsEqualTo(2);
        await Assert.That(await store.ReadPendingAsync(new(3, now), rows, default)).IsEqualTo(0);
        await Assert.That(await store.ReadPendingAsync(new(null, now), Memory<OutboxRecord>.Empty, default)).IsEqualTo(0);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.ReadPendingAsync(new(-1, now), rows, default));
    }

    [Test]
    [Arguments("missing", false)]
    [Arguments("future", false)]
    [Arguments("parked", false)]
    [Arguments("live", false)]
    [Arguments("due", true)]
    [Arguments("expired", true)]
    public async Task Reads_counts_and_claims_share_eligibility_at_the_same_utc_instant(string state, bool eligible)
    {
        await using var fixture = await CreateFixtureAsync();
        var store = fixture.Store();
        var now = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var id = Guid.CreateVersion7();
        if (state != "missing")
        {
            var expiry = state switch
            {
                "live" => now.AddSeconds(1),
                "expired" => (DateTimeOffset?)now,
                _ => null
            };
            await fixture.Insert(id, now, deliverAt: state == "future" ? now.AddSeconds(1) : now,
                status: state == "parked" ? "parked" : "pending", expires: expiry);
        }

        var queryTime = now.ToOffset(TimeSpan.FromHours(5));
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(null, queryTime), rows, default)).IsEqualTo(eligible ? 1 : 0);
        await Assert.That(await store.GetPendingCountAsync(queryTime, default)).IsEqualTo(eligible ? 1L : 0L);
        if (eligible) await Assert.That(rows[0].Id).IsEqualTo(id);
        await Assert.That(await store.TryClaimAsync(new(id, Guid.NewGuid()), queryTime, queryTime.AddMinutes(1), default))
            .IsEqualTo(eligible);
        await Assert.That(await store.GetPendingCountAsync(queryTime, default)).IsEqualTo(0L);
    }

    [Test]
    public async Task Concurrent_claims_fence_stale_owners_and_recover_at_exact_expiry()
    {
        await using var fixture = await CreateFixtureAsync();
        var a = fixture.Store();
        var b = fixture.Store();
        var now = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var id = Guid.CreateVersion7();
        await fixture.Insert(id, now);
        var first = new OutboxClaim(id, Guid.NewGuid());
        var second = new OutboxClaim(id, Guid.NewGuid());
        var outcomes = await Task.WhenAll(a.TryClaimAsync(first, now, now.AddMinutes(1), default).AsTask(),
            b.TryClaimAsync(second, now, now.AddMinutes(1), default).AsTask());
        await Assert.That(outcomes[0] ^ outcomes[1]).IsTrue();
        var winner = outcomes[0] ? first : second;
        var loser = outcomes[0] ? second : first;
        await Assert.That(await b.CompleteAsync(loser, now, default)).IsFalse();
        await Assert.That(await b.RenewClaimAsync(loser, now, now.AddMinutes(2), default)).IsFalse();
        await Assert.That(await b.RecordFailureAsync(loser, "stale", now, default)).IsFalse();
        await Assert.That(await a.RenewClaimAsync(winner, now, now.AddSeconds(1), default)).IsTrue();
        await Assert.That(await b.TryClaimAsync(loser, now.AddSeconds(2), now.AddMinutes(2), default)).IsFalse();
        var expiry = now.AddMinutes(1).ToOffset(TimeSpan.FromHours(5));
        await Assert.That(await a.CompleteAsync(winner, expiry, default)).IsFalse();
        await Assert.That(await a.RenewClaimAsync(winner, expiry, now.AddMinutes(2), default)).IsFalse();
        await Assert.That(await a.RecordFailureAsync(winner, "expired", expiry, default)).IsFalse();
        await Assert.That(await b.TryClaimAsync(loser, expiry, now.AddMinutes(2), default)).IsTrue();
        await Assert.That(await a.CompleteAsync(winner, now, default)).IsFalse();
        await Assert.That(await b.RecordFailureAsync(loser, "broker offline", expiry, default)).IsTrue();
        await Assert.That(await b.RecordFailureAsync(loser, "repeated", expiry, default)).IsFalse();
        var rows = new OutboxRecord[1];
        await Assert.That(await a.ReadPendingAsync(new(null, expiry), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Attempts).IsEqualTo(1);
        await Assert.That(rows[0].LastError).IsEqualTo("broker offline");
        var final = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await a.TryClaimAsync(final, expiry, now.AddMinutes(2), default)).IsTrue();
        await Assert.That(await a.CompleteAsync(final, expiry, default)).IsTrue();
        await Assert.That(await a.GetPendingCountAsync(expiry, default)).IsEqualTo(0L);
        await Assert.That(await a.CompleteAsync(final, expiry, default)).IsFalse();
    }

    [Test]
    public async Task Invalid_claims_and_cancellation_do_not_mutate_rows()
    {
        await using var fixture = await CreateFixtureAsync();
        var store = fixture.Store();
        var now = DateTimeOffset.UtcNow;
        var id = Guid.CreateVersion7();
        await fixture.Insert(id, now);
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.CompleteAsync(new(id, Guid.Empty), now, default));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.TryClaimAsync(new(Guid.Empty, Guid.NewGuid()), now, now.AddSeconds(1), default));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.TryClaimAsync(new(id, Guid.NewGuid()), now, now, default));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await store.RecordFailureAsync(new(id, Guid.NewGuid()), null!, now, default));
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await store.GetPendingCountAsync(now, new CancellationToken(true)));
        await Assert.That(await store.GetPendingCountAsync(now, default)).IsEqualTo(1L);
    }
}
