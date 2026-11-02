using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Relayer.Outbox.MongoDB.Tests;

public class ConfigurationTests
{
    [Test]
    public async Task Unacknowledged_writes_invalid_options_and_duplicate_names_fail_early()
    {
        using var client = new MongoClient("mongodb://localhost:1");
        var database = client.GetDatabase("test");
        await Assert.ThrowsAsync<ArgumentException>(() =>
        {
            _ = new MongoDBOutboxStore(database.WithWriteConcern(WriteConcern.Unacknowledged));
            return Task.CompletedTask;
        });
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
        {
            _ = new MongoDBOutbox(database, new() { Lanes = 0 });
            return Task.CompletedTask;
        });
        await Assert.ThrowsAsync<ArgumentException>(() =>
        {
            _ = new MongoDBOutboxStore(database, new() { CollectionPrefix = "$bad" });
            return Task.CompletedTask;
        });
        var services = new ServiceCollection().AddRelayerMongoDBOutboxStore("orders", database);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            services.AddRelayerMongoDBOutboxStore("orders", database);
            return Task.CompletedTask;
        });
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
        {
            services.AddRelayerMongoDBOutboxStore("null", database, (IConfiguration)null!);
            return Task.CompletedTask;
        });
    }

    [Test]
    public async Task Named_options_reload_for_new_scopes_and_conflicting_enlistments_fail()
    {
        using var client = new MongoClient("mongodb://localhost:1");
        var database = client.GetDatabase("test");
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["CollectionPrefix"] = "tenant_", ["Lanes"] = "8"
        }).Build();
        var services = new ServiceCollection().AddRelayerMongoDBOutbox("orders", database, configuration);
        services.AddRelayerMongoDBOutboxStore("other", database, new MongoDBOutboxOptions { Lanes = 4 });
        await Assert.ThrowsAsync<InvalidOperationException>(() => { services.AddRelayerMongoDBOutbox("conflict", database); return Task.CompletedTask; });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var first = provider.CreateAsyncScope();
        var before = first.ServiceProvider.GetRequiredService<IOptionsSnapshot<MongoDBOutboxOptions>>().Get("orders");
        await Assert.That(before.Lanes).IsEqualTo(8);
        await Assert.That(before.CollectionPrefix).IsEqualTo("tenant_");
        await Assert.That(first.ServiceProvider.GetRequiredService<IOutboxEnlistment>()).IsSameReferenceAs(first.ServiceProvider.GetRequiredService<MongoDBOutbox>());
        await Assert.That(first.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders")).IsTypeOf<MongoDBOutboxStore>();
        await Assert.That(first.ServiceProvider.GetRequiredService<IOptionsSnapshot<MongoDBOutboxOptions>>().Get("other").Lanes).IsEqualTo(4);
        configuration["Lanes"] = "16";
        configuration.Reload();
        await using var second = provider.CreateAsyncScope();
        await Assert.That(second.ServiceProvider.GetRequiredService<IOptionsSnapshot<MongoDBOutboxOptions>>().Get("orders").Lanes).IsEqualTo(16);
        await Assert.That(before.Lanes).IsEqualTo(8);
        configuration["Lanes"] = "bad";
        configuration.Reload();
        await using var third = provider.CreateAsyncScope();
        await Assert.ThrowsAsync<OptionsValidationException>(() => { third.ServiceProvider.GetRequiredService<MongoDBOutbox>(); return Task.CompletedTask; });
    }

    [Test]
    public async Task Reusable_BSON_encoding_handles_variable_bodies_without_managed_allocations()
    {
        var buffer = new MongoDBOutboxBuffer();
        var record = new OutboxRecord(Guid.CreateVersion7(), "events", "order;v=1", new("orders", new byte[4096],
            new KeyValuePair<string, WireString>[] { new("name", new("é")) }, new WireString("customer")), DateTimeOffset.UtcNow);
        for (var i = 0; i < 100; i++) buffer.Encode(record, 32);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            var message = record.Message;
            buffer.Encode(record with { Message = new(message.Destination, message.Body[..(i % 4097)], message.Headers, message.OrderingKey) }, 32);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        using var document = buffer.Write(record, 32);
        await Assert.That(document["body"].AsByteArray.Length).IsEqualTo(4096);
        await Assert.That(document["ordering_key"].AsByteArray.Length).IsEqualTo(8);
        await Assert.ThrowsAsync<ArgumentException>(() => { buffer.Encode(record with { Id = Guid.Empty }, 32); return Task.CompletedTask; });
    }
}
