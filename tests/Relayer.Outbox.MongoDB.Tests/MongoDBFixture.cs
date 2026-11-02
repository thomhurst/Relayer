using MongoDB.Bson;
using MongoDB.Driver;
using Testcontainers.MongoDb;

namespace Relayer.Outbox.MongoDB.Tests;

internal sealed class MongoDBFixture : IAsyncDisposable
{
    private readonly MongoDbContainer _container = new MongoDbBuilder("mongo:8.0.16").WithReplicaSet().Build();
    internal MongoClient Client { get; private set; } = null!;
    internal IMongoDatabase Database { get; private set; } = null!;
    internal MongoDBOutboxOptions Options { get; } = new() { CollectionPrefix = "custom_", Lanes = 8 };
    internal IMongoCollection<BsonDocument> Rows => Database.GetCollection<BsonDocument>(Options.CollectionPrefix + "outbox");

    internal async Task Start()
    {
        await _container.StartAsync();
        Client = new MongoClient(_container.GetConnectionString());
        Database = Client.GetDatabase("relayer");
        await MongoDBOutboxSchema.CreateAsync(Database, Options);
        // MongoDB requires collections participating in a transaction to exist before the transaction starts.
        await Database.CreateCollectionAsync("orders");
    }

    public async ValueTask DisposeAsync()
    {
        Client?.Dispose();
        await _container.DisposeAsync();
    }
}
