using Relayer.TestFixtures;

namespace Relayer.Outbox.SqlServer.Tests;

[InheritsTests]
[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class SharedStoreContractTests(SqlServerFixture database) : OutboxStoreContractTests
{
    protected override async Task<StoreFixture> CreateFixtureAsync()
    {
        var data = new StoreData(database);
        await data.Create();
        return new Fixture(data);
    }

    private sealed class Fixture(StoreData data) : StoreFixture
    {
        public override IOutboxStore Store() => data.Store();
        public override Task Insert(Guid id, DateTimeOffset now, int? lane = null, DateTimeOffset? deliverAt = null,
            string status = "pending", DateTimeOffset? expires = null) => data.Insert(id, now, lane, deliverAt, status, expires);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
