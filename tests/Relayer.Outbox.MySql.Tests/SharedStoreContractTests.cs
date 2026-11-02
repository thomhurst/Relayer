using Relayer.TestFixtures;

namespace Relayer.Outbox.MySql.Tests;

[InheritsTests]
public sealed class SharedStoreContractTests : OutboxStoreContractTests
{
    protected override async Task<StoreFixture> CreateFixtureAsync()
    {
        var fixture = new Fixture();
        try { await fixture.Database.StartAsync(); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    private sealed class Fixture : StoreFixture
    {
        internal MySqlFixture Database { get; } = new();
        private readonly List<MySqlOutboxStore> _stores = [];
        public override IOutboxStore Store()
        {
            var store = new MySqlOutboxStore(Database.Source);
            _stores.Add(store);
            return store;
        }
        public override Task Insert(Guid id, DateTimeOffset now, int? lane = null, DateTimeOffset? deliverAt = null,
            string status = "pending", DateTimeOffset? expires = null) =>
            Database.Insert(id, now, lane, deliverAt, status, expires);
        public override async ValueTask DisposeAsync()
        {
            foreach (var store in _stores) store.Dispose();
            await Database.DisposeAsync();
        }
    }
}
