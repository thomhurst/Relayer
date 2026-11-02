using Microsoft.Extensions.DependencyInjection;
using MySqlConnector;

namespace Relayer.Outbox.MySql.Tests;

public sealed class MySqlOutboxStoreTests
{
    [Test]
    public async Task Rows_preserve_payload_and_headers_across_repeated_reads_and_report_database_errors()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await database.Execute(MySqlOutboxSchema.CreateScript()); // idempotent deployment
        await database.Execute(MySqlOutboxSchema.CreateScript(new() { TablePrefix = new string('x', 52) }));
        var quotedOptions = new MySqlOutboxOptions { TablePrefix = "tenant`_" };
        await database.Execute(MySqlOutboxSchema.CreateScript(quotedOptions));
        using var store = new MySqlOutboxStore(database.Source);
        var now = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero).AddTicks(1234560);
        var id = Guid.CreateVersion7();
        await database.Insert(id, now, deliverAt: now);
        await database.Execute($"INSERT INTO {MySqlOutboxSchema.Table(quotedOptions)} SELECT * FROM relayer_outbox");
        using var quotedStore = new MySqlOutboxStore(database.Source, quotedOptions);
        await Assert.That(await quotedStore.GetPendingCountAsync(now, default)).IsEqualTo(1L);
        var rows = new OutboxRecord[1];
        for (var read = 0; read < 2; read++)
        {
            await Assert.That(await store.ReadPendingAsync(new(null, now), rows, default)).IsEqualTo(1);
            var row = rows[0];
            await Assert.That(row.Id).IsEqualTo(id);
            await Assert.That(row.CreatedAt).IsEqualTo(now);
            await Assert.That(row.Message.DeliverAt).IsEqualTo(now);
            await Assert.That(row.Message.Body.ToArray()).IsEquivalentTo(new byte[] { 0, 1, 255 });
            await Assert.That(row.Message.Headers.Length).IsEqualTo(3);
            await Assert.That(row.Message.Headers.Span[0].Key).IsEqualTo("duplicate");
            await Assert.That(row.Message.Headers.Span[1].Key).IsEqualTo("duplicate");
            await Assert.That(row.Message.Headers.Span[1].Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 255, 0, 254 });
            await Assert.That(row.Message.Headers.Span[2].Value.Utf8.Length).IsEqualTo(0);
            await Assert.That(row.Message.OrderingKey!.Value.ToString()).IsEqualTo("customer-é");
            await Assert.That(row.TenantId).IsEqualTo("tenant-é");
            await Assert.That(row.TransportName).IsEqualTo("events");
            await Assert.That(row.MessageType).IsEqualTo("order;v=1");
            await Assert.That(row.Message.Destination).IsEqualTo("orders");
        }
        await database.Execute("UPDATE relayer_outbox SET headers = X'00'");
        await Assert.ThrowsAsync<InvalidDataException>(async () => await store.ReadPendingAsync(new(null, now), rows, default));
        await database.Execute("DROP TABLE relayer_outbox");
        await Assert.ThrowsAsync<MySqlException>(async () => await store.GetPendingCountAsync(now, default));
    }

    [Test]
    public async Task Binary_ids_sort_in_network_order_across_uuid_field_boundaries()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        using var store = new MySqlOutboxStore(database.Source);
        var now = DateTimeOffset.UtcNow;
        var first = Guid.Parse("0190ffff-ffff-7fff-bfff-ffffffffffff");
        var second = Guid.Parse("01910000-0000-7000-8000-000000000000");
        await database.Insert(second, now);
        await database.Insert(first, now);
        var rows = new OutboxRecord[2];
        await Assert.That(await store.ReadPendingAsync(new(null, now), rows, default)).IsEqualTo(2);
        await Assert.That(rows[0].Id).IsEqualTo(first);
        await Assert.That(rows[1].Id).IsEqualTo(second);
        await Assert.That(await store.ReadPendingAsync(new(null, now, first), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(second);
    }

    [Test]
    public async Task Registration_validates_native_settings_and_preserves_scopes_and_source_ownership()
    {
        await using var source = new MySqlDataSource("Server=unused;Database=relayer");
        var services = new ServiceCollection().AddRelayerMySqlOutboxStore("orders", source);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            services.AddRelayerMySqlOutboxStore("orders", source);
            return Task.CompletedTask;
        });
        using (var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true }))
        {
            using var first = provider.CreateScope();
            using var second = provider.CreateScope();
            var store = first.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders");
            await Assert.That(store).IsSameReferenceAs(first.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders"));
            await Assert.That(ReferenceEquals(store, second.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders"))).IsFalse();
        }
        using var connection = source.CreateConnection(); // registration did not dispose source
        await using var incompatible = new MySqlDataSource("Server=unused;UseAffectedRows=true");
        await Assert.ThrowsAsync<ArgumentException>(() =>
        {
            new ServiceCollection().AddRelayerMySqlOutboxStore("invalid", incompatible);
            return Task.CompletedTask;
        });
        await Assert.ThrowsAsync<ArgumentException>(() =>
        {
            using var invalid = new MySqlOutboxStore(incompatible);
            return Task.CompletedTask;
        });
    }

    [Test]
    public async Task Schema_quotes_prefixes_and_rejects_invalid_identifiers()
    {
        await Assert.That(MySqlOutboxSchema.CreateScript(new() { TablePrefix = "tenant`_" })).Contains("`tenant``_outbox`");
        foreach (var prefix in new[] { new string('x', 53), "bad\0", "😀" })
            await Assert.ThrowsAsync<ArgumentException>(() =>
            {
                _ = MySqlOutboxSchema.CreateScript(new() { TablePrefix = prefix });
                return Task.CompletedTask;
            });
    }

    [Test]
    public async Task Warm_parameter_updates_do_not_allocate_and_use_utc()
    {
        var parameters = new MySqlStoreParameters("SELECT 1");
        using var owned = parameters.Command;
        var now = new DateTimeOffset(2030, 1, 2, 3, 4, 5, TimeSpan.FromHours(5));
        var claim = new OutboxClaim(Guid.NewGuid(), Guid.NewGuid());
        for (var i = 0; i < 100; i++) Update();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) Update();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        var timestamp = (ReadOnlyMemory<char>)owned.Parameters["now"].Value!;
        await Assert.That(timestamp.ToString()).IsEqualTo("2030-01-01 22:04:05.000000");
        void Update() { parameters.Now(now); parameters.Expires(now.AddMinutes(1)); parameters.Claim(claim); parameters.Read(2, claim.MessageId, 64); }
    }
}
