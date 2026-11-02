using System.Data;
using System.Transactions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class SqlServerOutboxStoreTests(SqlServerFixture database)
{
    [Test]
    public async Task Binary_ids_use_uuid_order_instead_of_uniqueidentifier_order_and_preserve_payloads()
    {
        var data = new StoreData(database, new() { Schema = "O'];--{{schema}}", TablePrefix = "a']_" });
        await data.Create();
        await data.Create(); // Idempotent, including escaped SQL literal and identifier contexts.
        var now = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.FromHours(5)).AddTicks(1);
        var first = Guid.Parse("01900000-0000-7000-8000-ffffffffffff");
        var second = Guid.Parse("01900000-0001-7000-8000-000000000000");
        await data.Insert(second, now, bodySize: 100_000);
        await data.Insert(first, now, deliverAt: now);
        var store = data.Store();
        var rows = new OutboxRecord[2];
        await Assert.That(await store.ReadPendingAsync(new(null, now), rows, default)).IsEqualTo(2);
        await Assert.That(rows[0].Id).IsEqualTo(first);
        await Assert.That(rows[1].Id).IsEqualTo(second);
        await Assert.That(rows[0].Message.Body.ToArray()).IsEquivalentTo(new byte[] { 0, 255, 1 });
        await Assert.That(rows[1].Message.Body.Length).IsEqualTo(100_000);
        await Assert.That(rows[0].Message.Headers.Length).IsEqualTo(3);
        await Assert.That(rows[0].Message.Headers.Span[0].Key).IsEqualTo("duplicate");
        await Assert.That(rows[0].Message.Headers.Span[0].Value.ToString()).IsEqualTo("é");
        await Assert.That(rows[0].Message.Headers.Span[1].Key).IsEqualTo("duplicate");
        await Assert.That(rows[0].Message.Headers.Span[1].Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 255, 0, 254 });
        await Assert.That(rows[0].Message.Headers.Span[2].Value.IsEmpty).IsTrue();
        await Assert.That(rows[0].Message.Destination).IsEqualTo("orders");
        await Assert.That(rows[0].MessageType).IsEqualTo("order.placed.v1");
        await Assert.That(rows[0].TransportName).IsEqualTo("events");
        await Assert.That(rows[0].Message.OrderingKey!.Value.ToString()).IsEqualTo("customer-é");
        await Assert.That(rows[0].TenantId).IsEqualTo("tenant-é");
        await Assert.That(rows[0].CreatedAt).IsEqualTo(now);
        await Assert.That(rows[0].Message.DeliverAt).IsEqualTo(now);
        await Assert.That(await store.ReadPendingAsync(new(null, now, first), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(second);
        await Assert.That(rows[0].Message.Body.Length).IsEqualTo(100_000);
        await Assert.That(await store.ReadPendingAsync(new(null, now, second), rows, default)).IsEqualTo(0);
    }

    [Test]
    public async Task Keyed_scopes_are_distinct_and_database_errors_propagate()
    {
        var data = new StoreData(database);
        await data.Create();
        var services = new ServiceCollection();
        services.AddRelayerSqlServerOutboxStore("orders", database.ConnectionString, data.Options);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders");
        await Assert.That(store).IsSameReferenceAs(scope.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders"));
        await using var other = provider.CreateAsyncScope();
        await Assert.That(store).IsNotSameReferenceAs(other.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders"));
        await Assert.That(await store.GetPendingCountAsync(DateTimeOffset.UtcNow, default)).IsEqualTo(0L);
        await data.Execute($"DROP TABLE {data.Table}");
        await Assert.ThrowsAsync<SqlException>(async () => await store.GetPendingCountAsync(DateTimeOffset.UtcNow, default));
    }

    [Test]
    public async Task Schema_rejects_invalid_rows_and_corrupt_headers_fail_explicitly()
    {
        var data = new StoreData(database);
        await data.Create();
        var now = DateTimeOffset.UtcNow;
        await Assert.ThrowsAsync<SqlException>(() => data.Insert(Guid.Empty, now));
        await Assert.ThrowsAsync<SqlException>(() => data.Insert(Guid.CreateVersion7(), now, lane: -1));
        await Assert.ThrowsAsync<SqlException>(() => data.Insert(Guid.CreateVersion7(), now, status: "dead"));
        await data.Insert(Guid.CreateVersion7(), now);
        await Assert.ThrowsAsync<SqlException>(() => data.Execute($"UPDATE {data.Table} SET attempts = -1"));
        await Assert.ThrowsAsync<SqlException>(() => data.Execute($"UPDATE {data.Table} SET claim_expires_at = SYSDATETIMEOFFSET()"));
        await data.Execute($"UPDATE {data.Table} SET headers = 0x00");
        await Assert.ThrowsAsync<InvalidDataException>(async () => await data.Store().ReadPendingAsync(new(null, now), new OutboxRecord[1], default));
    }

    [Test]
    public async Task Ambient_transactions_are_rejected_before_native_enlistment()
    {
        using var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        var store = new SqlServerOutboxStore(database.ConnectionString);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.GetPendingCountAsync(DateTimeOffset.UtcNow, default));
    }

    [Test]
    public async Task Native_cancellation_interrupts_a_blocked_claim_and_the_store_remains_usable()
    {
        var data = new StoreData(database);
        await data.Create();
        var now = DateTimeOffset.UtcNow;
        var claim = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        await data.Insert(claim.MessageId, now);
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        using (var blocker = new SqlCommand($"SELECT id FROM {data.Table} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id", connection, transaction))
        {
            blocker.Parameters.Add("@id", SqlDbType.Binary, 16).Value = claim.MessageId.ToByteArray(bigEndian: true);
            await blocker.ExecuteScalarAsync();
        }
        var store = data.Store();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var pending = store.TryClaimAsync(claim, now, now.AddMinutes(1), cancellation.Token).AsTask();
        using var monitor = new SqlCommand("SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id = @@SPID", connection, transaction);
        // Observe the native lock wait, rather than assuming a delay was long enough to reach the server.
        while ((int)(await monitor.ExecuteScalarAsync(cancellation.Token))! == 0)
            await Task.Delay(10, cancellation.Token);
        await cancellation.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await pending);
        await transaction.RollbackAsync();
        await Assert.That(await store.GetPendingCountAsync(now, default)).IsEqualTo(1L);
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
    }
}
