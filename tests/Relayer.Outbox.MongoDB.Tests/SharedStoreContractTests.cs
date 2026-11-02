using MongoDB.Bson;
using Relayer.TestFixtures;

namespace Relayer.Outbox.MongoDB.Tests;

[InheritsTests]
public sealed class SharedStoreContractTests : OutboxStoreContractTests
{
    protected override async Task<StoreFixture> CreateFixtureAsync()
    {
        var fixture = new Fixture();
        try { await fixture.Database.Start(); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    private sealed class Fixture : StoreFixture
    {
        internal MongoDBFixture Database { get; } = new();
        public override IOutboxStore Store() => new MongoDBOutboxStore(Database.Database, Database.Options);
        public override async Task Insert(Guid id, DateTimeOffset now, int? lane = null, DateTimeOffset? deliverAt = null,
            string status = "pending", DateTimeOffset? expires = null)
        {
            using var raw = new MongoDBOutboxBuffer().Write(new(id, "events", "order;v=1", new("orders", new byte[] { 1 }, deliverAt: deliverAt), now), 8);
            var row = new BsonDocument(raw.Elements);
            row["lane"] = lane is { } value ? new BsonInt32(value) : BsonNull.Value;
            row["status"] = status;
            if (expires is { } expiry) row["claim_expires_at_utc_ticks"] = expiry.UtcTicks;
            await Database.Rows.InsertOneAsync(row);
        }
        public override ValueTask DisposeAsync() => Database.DisposeAsync();
    }
}
