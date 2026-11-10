using MongoDB.Bson;
using MongoDB.Driver;
using Testcontainers.MongoDb;

namespace Relayer.Outbox.MongoDB.Tests;

public class SchemaTests
{
    [Test]
    public async Task Explicit_deployment_is_idempotent_and_creates_pending_and_delivery_indexes()
    {
        await using var fixture = new MongoDBFixture();
        await fixture.Start();
        await MongoDBOutboxSchema.CreateAsync(fixture.Database, fixture.Options);
        using var indexes = await fixture.Rows.Indexes.ListAsync();
        var definitions = await indexes.ToListAsync();
        await Assert.That(definitions.Count).IsEqualTo(3);
        var pending = definitions.Single(index => index["name"] == "pending_lane");
        await Assert.That(pending["partialFilterExpression"]["status"].AsString).IsEqualTo("pending");
        await Assert.That(pending["key"].AsBsonDocument.ElementCount).IsEqualTo(2);
        await Assert.That(definitions.Any(index => index["name"] == "deliver_at")).IsTrue();
    }

    [Test]
    public async Task Standalone_server_fails_with_actionable_transaction_requirement()
    {
        await using var container = new MongoDbBuilder("mongo:8.0.16").Build();
        await container.StartAsync();
        using var client = new MongoClient(container.GetConnectionString());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await MongoDBOutboxSchema.CreateAsync(client.GetDatabase("relayer")));
    }

    [Test]
    public async Task Corrupt_header_lengths_fail_without_unbounded_allocation()
    {
        await using var fixture = new MongoDBFixture();
        await fixture.Start();
        var outbox = new MongoDBOutbox(fixture.Database, fixture.Options);
        using var session = await fixture.Client.StartSessionAsync();
        session.StartTransaction();
        using (outbox.EnlistTransaction(session)) await outbox.EnlistAsync(EnlistmentTests.Record(Guid.CreateVersion7()), default);
        await session.CommitTransactionAsync();
        await fixture.Rows.UpdateOneAsync(new BsonDocument(), new BsonDocument("$set", new BsonDocument("headers", new byte[] { 255, 255, 255, 127 })));
        var store = new MongoDBOutboxStore(fixture.Database, fixture.Options);
        await Assert.ThrowsAsync<FormatException>(async () => await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), new OutboxRecord[1], default));
    }
}
