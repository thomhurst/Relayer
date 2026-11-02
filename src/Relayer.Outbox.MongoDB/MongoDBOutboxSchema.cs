using MongoDB.Bson;
using MongoDB.Driver;

namespace Relayer.Outbox.MongoDB;

/// <summary>Explicit, idempotent collection and index deployment. Registration does not deploy schema.</summary>
public static class MongoDBOutboxSchema
{
    /// <summary>Validates transaction support and creates the outbox collection and indexes.</summary>
    /// <remarks>Run outside application transactions. Requires a replica set or sharded cluster.</remarks>
    public static async Task CreateAsync(IMongoDatabase database, MongoDBOutboxOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        options ??= new();
        options.Validate();
        MongoDBSerializerRoots.Preserve();
        var hello = await database.RunCommandAsync<BsonDocument>(new BsonDocument("hello", 1), cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!hello.Contains("logicalSessionTimeoutMinutes") ||
            (!hello.Contains("setName") && hello.GetValue("msg", "") != "isdbgrid") || hello["maxWireVersion"].AsInt32 < 7)
            throw new InvalidOperationException("MongoDB outbox transactions require a transaction-capable replica set or sharded cluster. A standalone server is not supported.");
        var name = options.CollectionPrefix + "outbox";
        try { await database.RunCommandAsync<BsonDocument>(new BsonDocument("create", name), cancellationToken: cancellationToken).ConfigureAwait(false); }
        catch (MongoCommandException error) when (error.Code == 48) { } // Another deployer already created this namespace.
        var collection = database.GetCollection<BsonDocument>(name);
        await collection.Indexes.CreateManyAsync([
            new CreateIndexModel<BsonDocument>(new BsonDocument { { "lane", 1 }, { "_id", 1 } },
                new CreateIndexOptions<BsonDocument> { Name = "pending_lane", PartialFilterExpression = new BsonDocument("status", "pending") }),
            new CreateIndexModel<BsonDocument>(new BsonDocument("deliver_at_utc_ticks", 1), new CreateIndexOptions { Name = "deliver_at" })
        ], cancellationToken).ConfigureAwait(false);
    }
}
