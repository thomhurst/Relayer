using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
using Relayer;
using Relayer.Outbox.MongoDB;

// Separate from the passing core smoke: MongoDB.Driver 3.13.0 currently fails strict AOT analysis.
// Keep that gate intact. This executable is also the standalone reproduction for the upstream blocker.
using var client = new MongoClient(args[0]);
var database = client.GetDatabase("relayer_aot_" + Guid.NewGuid().ToString("N"));
try
{
    var services = new ServiceCollection().AddRelayerMongoDBOutbox("mongo", database,
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["Lanes"] = "8" }).Build());
    await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    await using var scope = provider.CreateAsyncScope();
    var outbox = scope.ServiceProvider.GetRequiredService<MongoDBOutbox>();
    var store = scope.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("mongo");
    await MongoDBOutboxSchema.CreateAsync(database);
    await database.RunCommandAsync<BsonDocument>(new BsonDocument("create", "business"));
    var id = Guid.CreateVersion7();
    var now = DateTimeOffset.UtcNow;
    using (var session = await client.StartSessionAsync())
    {
        session.StartTransaction(new TransactionOptions(writeConcern: WriteConcern.WMajority));
        using var association = outbox.EnlistTransaction(session);
        await database.GetCollection<BsonDocument>("business").InsertOneAsync(session, new BsonDocument("_id", "order"));
        await outbox.EnlistAsync(new(id, "events", "order;v=1", new("orders", new byte[] { 1 },
            new KeyValuePair<string, WireString>[] { new("creation-context", new("original")) }), now), default);
        if (await store.GetPendingCountAsync(now, default) != 0) throw new Exception("Uncommitted outbox row became visible.");
        await session.CommitTransactionAsync();
    }
    var rows = new OutboxRecord[1];
    if (await store.ReadPendingAsync(new(null, now), rows, default) != 1 || rows[0].Id != id || rows[0].Message.Headers.Span[0].Value.ToString() != "original")
        throw new Exception("Native AOT projection failed.");
    var claim = new OutboxClaim(id, Guid.NewGuid());
    if (!await store.TryClaimAsync(claim, now, now.AddMinutes(1), default) ||
        !await store.RenewClaimAsync(claim, now, now.AddMinutes(2), default) ||
        !await store.RecordFailureAsync(claim, "retry", now, default)) throw new Exception("Native AOT fenced mutation failed.");
    claim = new(id, Guid.NewGuid());
    if (!await store.TryClaimAsync(claim, now, now.AddMinutes(1), default) ||
        !await store.CompleteAsync(claim, now, default) || await store.GetPendingCountAsync(now, default) != 0)
        throw new Exception("Native AOT completion failed.");
    Console.WriteLine("MongoDB persistence native transaction, read and fenced mutations passed under Native AOT.");
}
finally { await client.DropDatabaseAsync(database.DatabaseNamespace.DatabaseName); }
