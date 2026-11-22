using Npgsql;
using Relayer.TestFixtures;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

[InheritsTests]
public sealed class SharedStoreContractTests : OutboxStoreContractTests
{
    protected override async Task<StoreFixture> CreateFixtureAsync()
    {
        var fixture = new Fixture();

        try
        {
            await fixture.Start();
            return fixture;
        }
        catch
        {
            await fixture.DisposeAsync();
            throw;
        }
    }

    private sealed class Fixture : StoreFixture
    {
        private readonly PostgreSqlFixture _database = new();
        private NpgsqlDataSource? _source;

        public async Task Start()
        {
            await _database.StartAsync();
            _source = NpgsqlDataSource.Create(_database.ConnectionString);
            await PostgreSqlOutboxStoreTests.CreateSchema(_source);
        }

        public override IOutboxStore Store()
        {
            return new PostgreSqlOutboxStore(_source!);
        }

        public override Task Insert(Guid id, DateTimeOffset now, int? lane = null, DateTimeOffset? deliverAt = null,
            string status = "pending", DateTimeOffset? expires = null)
        {
            return PostgreSqlOutboxStoreTests.Insert(_source!, new(), id, now, lane, deliverAt, status, expires);
        }

        public override async ValueTask DisposeAsync()
        {
            if (_source is not null)
            {
                await _source.DisposeAsync();
            }

            await _database.DisposeAsync();
        }
    }
}
