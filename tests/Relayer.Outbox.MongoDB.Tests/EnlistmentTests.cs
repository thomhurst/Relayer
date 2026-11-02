using MongoDB.Bson;
using MongoDB.Driver;

namespace Relayer.Outbox.MongoDB.Tests;

public class EnlistmentTests
{
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Business_writes_and_owned_envelopes_commit_or_abort_together(bool commit)
    {
        await using var fixture = new MongoDBFixture();
        await fixture.Start();
        var outbox = new MongoDBOutbox(fixture.Database, fixture.Options);
        var store = new MongoDBOutboxStore(fixture.Database, fixture.Options);
        using var session = await fixture.Client.StartSessionAsync();
        session.StartTransaction(new TransactionOptions(writeConcern: WriteConcern.WMajority));
        using var association = outbox.EnlistTransaction(session);
        byte[] body = [0, 255, 1];
        byte[] key = [0xff, 0, 0xfe];
        byte[] value = [255, 0, 254];
        KeyValuePair<string, WireString>[] headers = [new("duplicate", new("é")), new("duplicate", new(value)), new("empty", new(""))];
        var now = DateTimeOffset.UtcNow.ToOffset(TimeSpan.FromHours(5)).AddTicks(1);
        var id = Guid.CreateVersion7();
        var record = new OutboxRecord(id, "events", "order;v=1", new("orders", body, headers, new WireString(key), now), now, TenantId: "tenant-é");
        var orders = fixture.Database.GetCollection<BsonDocument>("orders");
        await orders.InsertOneAsync(session, new BsonDocument("_id", "business"));
        await outbox.EnlistAsync(record, default);
        body.AsSpan().Clear(); value.AsSpan().Clear(); key.AsSpan().Clear(); Array.Clear(headers);
        await Assert.That(await store.GetPendingCountAsync(now, default)).IsEqualTo(0L);
        await Assert.That(await orders.CountDocumentsAsync(new BsonDocument())).IsEqualTo(0L);
        if (commit) await session.CommitTransactionAsync(); else await session.AbortTransactionAsync();
        await Assert.That(outbox.IsActive).IsFalse();
        await Assert.That(await orders.CountDocumentsAsync(new BsonDocument())).IsEqualTo(commit ? 1L : 0L);
        await Assert.That(await store.GetPendingCountAsync(now, default)).IsEqualTo(commit ? 1L : 0L);
        if (!commit) return;
        // Inspect only the lane field natively; all envelope assertions use the portable projection.
        var native = await fixture.Rows.Find(new BsonDocument()).SingleAsync();
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(native["lane"].AsInt32, now), rows, default)).IsEqualTo(1);
        var result = rows[0];
        await Assert.That(result.Id).IsEqualTo(id);
        await Assert.That(result.MessageType).IsEqualTo("order;v=1");
        await Assert.That(result.TransportName).IsEqualTo("events");
        await Assert.That(result.Message.Destination).IsEqualTo("orders");
        await Assert.That(result.CreatedAt).IsEqualTo(now);
        await Assert.That(result.Message.DeliverAt).IsEqualTo(now);
        await Assert.That(result.TenantId).IsEqualTo("tenant-é");
        await Assert.That(result.Message.Body.ToArray()).IsEquivalentTo(new byte[] { 0, 255, 1 });
        await Assert.That(result.Message.OrderingKey!.Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 255, 0, 254 });
        await Assert.That(result.Message.Headers.Length).IsEqualTo(3);
        await Assert.That(result.Message.Headers.Span[0].Key).IsEqualTo("duplicate");
        await Assert.That(result.Message.Headers.Span[0].Value.Utf8.ToArray()).IsEquivalentTo(System.Text.Encoding.UTF8.GetBytes("é"));
        await Assert.That(result.Message.Headers.Span[1].Key).IsEqualTo("duplicate");
        await Assert.That(result.Message.Headers.Span[1].Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 255, 0, 254 });
        await Assert.That(result.Message.Headers.Span[2].Value.IsEmpty).IsTrue();
        await Assert.That(await store.GetPendingCountAsync(now.AddTicks(-1), default)).IsEqualTo(0L);
    }

    [Test]
    public async Task Native_retry_replays_complete_unit_without_retaining_aborted_messages()
    {
        await using var fixture = new MongoDBFixture();
        await fixture.Start();
        var outbox = new MongoDBOutbox(fixture.Database, fixture.Options);
        using var session = await fixture.Client.StartSessionAsync();
        var attempts = 0;
        var aborted = Guid.CreateVersion7();
        var committed = Guid.CreateVersion7();
        await session.WithTransactionAsync(async (native, cancellationToken) =>
        {
            using var association = outbox.EnlistTransaction(native);
            await outbox.EnlistAsync(Record(++attempts == 1 ? aborted : committed), cancellationToken);
            if (attempts == 1)
            {
                var conflict = new MongoException("Injected transient transaction conflict");
                conflict.AddErrorLabel("TransientTransactionError");
                throw conflict;
            }
            return true;
        });
        await Assert.That(attempts).IsEqualTo(2);
        var rows = new OutboxRecord[2];
        await Assert.That(await new MongoDBOutboxStore(fixture.Database, fixture.Options).ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(committed);
    }

    [Test]
    public async Task Native_transaction_replacement_requires_a_new_association()
    {
        await using var fixture = new MongoDBFixture();
        await fixture.Start();
        var outbox = new MongoDBOutbox(fixture.Database, fixture.Options);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await outbox.EnlistAsync(Record(Guid.CreateVersion7()), default));
        using var session = await fixture.Client.StartSessionAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => { outbox.EnlistTransaction(session); return Task.CompletedTask; });
        session.StartTransaction();
        using (outbox.EnlistTransaction(session))
        {
            await outbox.EnlistAsync(Record(Guid.CreateVersion7()), default);
            await session.CommitTransactionAsync();
            session.StartTransaction();
            await Assert.That(outbox.IsActive).IsFalse();
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await outbox.EnlistAsync(Record(Guid.CreateVersion7()), default));
            await session.AbortTransactionAsync();
        }
        session.StartTransaction();
        using (outbox.EnlistTransaction(session)) await outbox.EnlistAsync(Record(Guid.CreateVersion7()), default);
        await session.AbortTransactionAsync();
        await Assert.That(await fixture.Rows.CountDocumentsAsync(new BsonDocument())).IsEqualTo(1L);
    }

    internal static OutboxRecord Record(Guid id) => new(id, "events", "order;v=1", new("orders", new byte[] { 1 }), DateTimeOffset.UtcNow);
}
