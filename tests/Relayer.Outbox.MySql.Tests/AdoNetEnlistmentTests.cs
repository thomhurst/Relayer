using System.Data;
using System.Data.Common;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace Relayer.Outbox.MySql.Tests;

public class AdoNetEnlistmentTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Dapper_and_buffered_sends_commit_atomically_with_owned_payloads(bool synchronous)
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await database.Execute("CREATE TABLE business (id INT PRIMARY KEY) ENGINE=InnoDB");
        await using var provider = Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await Assert.That(enlistment).IsSameReferenceAs(outbox);
        await Assert.That(enlistment.SupportsScheduling).IsFalse();
        await Assert.That(enlistment.SupportsOrderedScheduling).IsFalse();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using DbTransaction transaction = await outbox.BeginTransactionAsync(connection, IsolationLevel.ReadCommitted);
        await Assert.That(transaction.IsolationLevel).IsEqualTo(IsolationLevel.ReadCommitted);
        if (synchronous) transaction.Connection!.Execute("INSERT INTO business VALUES (@id)", new { id = 1 }, transaction);
        else await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (@id)", new { id = 1 }, transaction);
        var body = new byte[] { 1, 255 };
        var header = new byte[] { 255, 0, 254 };
        var key = "customer-é"u8.ToArray();
        var created = new DateTimeOffset(2030, 1, 1, 2, 0, 0, TimeSpan.FromHours(2));
        var record = new OutboxRecord(Guid.CreateVersion7(), "events", "order;v=1",
            new OutgoingMessage("orders", body, new KeyValuePair<string, WireString>[] { new("duplicate", new(header)), new("duplicate", new("é")) }, new WireString(key), created.AddDays(1)),
            created, TenantId: "tenant-é");
        await enlistment.EnlistAsync(record, default);
        body[0] = 99;
        header[0] = 99;
        key[0] = 99;
        for (var i = 0; i < 128; i++) await enlistment.EnlistAsync(Record(), default);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(0L);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM business")).IsEqualTo(0L);
        if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
        await Assert.That(enlistment.IsActive).IsFalse();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(129L);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM business")).IsEqualTo(1L);
        using var store = new MySqlOutboxStore(database.Source);
        var buffer = new OutboxRecord[1];
        var owned = new BufferedOutboxMessage();
        owned.Copy(record with { Message = new OutgoingMessage("orders", new byte[] { 1, 255 }, orderingKey: new WireString("customer-é")) }, 8);
        await Assert.That(await store.ReadPendingAsync(new(owned.Lane, created.AddDays(2)), buffer, default)).IsEqualTo(1);
        var actual = buffer[0];
        await Assert.That(actual.Id).IsEqualTo(record.Id);
        await Assert.That(actual.Message.Body.ToArray()).IsEquivalentTo(new byte[] { 1, 255 });
        await Assert.That(actual.Message.Headers.Span[0].Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 255, 0, 254 });
        await Assert.That(actual.Message.Headers.Length).IsEqualTo(2);
        await Assert.That(actual.Message.OrderingKey!.Value.ToString()).IsEqualTo("customer-é");
        await Assert.That(actual.CreatedAt).IsEqualTo(created.ToUniversalTime());
        await Assert.That(actual.Message.DeliverAt).IsEqualTo(created.AddDays(1).ToUniversalTime());
        await Assert.That(actual.TenantId).IsEqualTo("tenant-é");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Rollback_disposal_and_failed_native_batch_leave_no_partial_work(bool synchronous)
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await database.Execute("CREATE TABLE business (id INT PRIMARY KEY) ENGINE=InnoDB");
        await using var provider = Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        for (var mode = 0; mode < 3; mode++)
        {
            var transaction = await outbox.BeginTransactionAsync(database.Source);
            await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: transaction);
            var row = Record();
            await outbox.EnlistAsync(row, default);
            if (mode == 0)
            {
                if (synchronous) transaction.Rollback(); else await transaction.RollbackAsync();
            }
            if (mode == 2)
            {
                // The first insert succeeds, then the second fails. Native disposal must roll back both.
                await outbox.EnlistAsync(row, default);
                if (synchronous) await Assert.That(() => transaction.Commit()).Throws<MySqlException>();
                else await Assert.That(async () => await transaction.CommitAsync()).Throws<MySqlException>();
            }
            if (synchronous) transaction.Dispose(); else await transaction.DisposeAsync();
            await Assert.That(outbox.IsActive).IsFalse();
            await Assert.That(transaction.Connection).IsNull();
            await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(0L);
            await Assert.That(await Scalar(database, "SELECT count(*) FROM business")).IsEqualTo(0L);
        }
        await using var next = await outbox.BeginTransactionAsync(database.Source);
        await outbox.EnlistAsync(Record(), default);
        await next.CommitAsync();
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(1L);
    }

    [Test]
    public async Task Cancellation_rolls_back_and_allows_next_unit()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await database.Execute("CREATE TABLE business (id INT PRIMARY KEY) ENGINE=InnoDB");
        await using var provider = Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await outbox.BeginTransactionAsync(database.Source, cancellationToken: canceled.Token)).Throws<OperationCanceledException>();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using (var transaction = await outbox.BeginTransactionAsync(connection))
        {
            await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: transaction);
            await Assert.That(async () => await outbox.EnlistAsync(Record(), canceled.Token)).Throws<OperationCanceledException>();
            await outbox.EnlistAsync(Record(), default);
            await Assert.That(async () => await transaction.CommitAsync(canceled.Token)).Throws<OperationCanceledException>();
        }
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM business")).IsEqualTo(0L);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(0L);
        await using var next = await outbox.BeginTransactionAsync(connection);
        await next.CommitAsync();
    }

    [Test]
    public async Task Native_batch_reuses_binary_slices_across_payload_sizes_and_optional_fields()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await using var provider = Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        foreach (var size in new[] { 4096, 0, 7, 16, 2 })
        {
            var bytes = new byte[size];
            Array.Fill(bytes, (byte)size);
            var record = Record() with { Message = new OutgoingMessage("orders", bytes) };
            await using var transaction = await outbox.BeginTransactionAsync(database.Source);
            await outbox.EnlistAsync(record, default);
            await transaction.CommitAsync();
            await using var command = database.Source.CreateCommand("SELECT body FROM relayer_outbox WHERE id=@id");
            command.Parameters.Add(new MySqlParameter("id", record.Id.ToByteArray(bigEndian: true)));
            await Assert.That((byte[])(await command.ExecuteScalarAsync())!).IsEquivalentTo(bytes);
        }
    }

    [Test]
    public async Task Cancellation_during_native_batch_rolls_back_earlier_inserts_and_business_work()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await database.Execute("CREATE TABLE business (id INT PRIMARY KEY) ENGINE=InnoDB");
        var blocked = Record();
        await using var blocker = await database.Source.OpenConnectionAsync();
        await using var hold = await blocker.BeginTransactionAsync();
        await using (var insert = new MySqlCommand("""
            INSERT INTO relayer_outbox (id, transport, destination, message_type, body, headers, created_at)
            VALUES (@id, 'events', 'orders', 'order;v=1', X'01', X'0100000000000000', UTC_TIMESTAMP(6))
            """, blocker, hold))
        {
            insert.Parameters.AddWithValue("id", blocked.Id.ToByteArray(bigEndian: true));
            await insert.ExecuteNonQueryAsync();
        }
        await using var provider = Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await outbox.BeginTransactionAsync(connection);
        await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: transaction);
        await outbox.EnlistAsync(Record(), default);
        await outbox.EnlistAsync(blocked, default);
        using var cancellation = new CancellationTokenSource();
        var commit = transaction.CommitAsync(cancellation.Token);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await using var observer = database.Source.CreateCommand("SELECT count(*) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID = w.REQUESTING_ENGINE_LOCK_ID WHERE l.OBJECT_SCHEMA = DATABASE()");
            while (Convert.ToInt64(await observer.ExecuteScalarAsync(timeout.Token)) == 0)
                await Task.Delay(10, timeout.Token);
        }
        finally
        {
            cancellation.Cancel();
            await Assert.That(async () => await commit).Throws<OperationCanceledException>();
        }
        await hold.RollbackAsync();
        await Assert.That(outbox.IsActive).IsFalse();
        await Assert.That(await Scalar(database, "SELECT count(*) FROM business")).IsEqualTo(0L);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(0L);
        await using var next = await outbox.BeginTransactionAsync(connection);
        await next.CommitAsync();
    }

    [Test]
    public async Task Connection_view_rejects_foreign_transactions_savepoints_and_reuse()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await using var provider = Services();
        await using var scope = provider.CreateAsyncScope();
        await using var otherScope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        await Assert.That(async () => await outbox.EnlistAsync(Record(), default)).Throws<InvalidOperationException>();
        await using var transaction = await outbox.BeginTransactionAsync(database.Source);
        await Assert.That(async () => await outbox.BeginTransactionAsync(database.Source)).Throws<InvalidOperationException>();
        await using var other = await otherScope.ServiceProvider.GetRequiredService<MySqlOutbox>().BeginTransactionAsync(database.Source);
        var view = transaction.Connection!;
        await using var command = view.CreateCommand();
        command.CommandText = "SELECT 42";
        await Assert.That(() => command.Transaction = other).Throws<InvalidOperationException>();
        await Assert.That(() => view.Close()).Throws<InvalidOperationException>();
        await Assert.That(() => view.BeginTransaction()).Throws<InvalidOperationException>();
        await Assert.That(() => transaction.Save("point")).Throws<NotSupportedException>();
        await Assert.That(async () => await command.ExecuteReaderAsync(CommandBehavior.CloseConnection)).Throws<NotSupportedException>();
        await view.DisposeAsync();
        command.Transaction = null;
        await Assert.That(Convert.ToInt32(await command.ExecuteScalarAsync())).IsEqualTo(42);
        await transaction.CommitAsync();
        await Assert.That(async () => await command.ExecuteScalarAsync()).Throws<InvalidOperationException>();
        await Assert.That(() => view.CreateCommand()).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Disposing_scope_rolls_back_and_preserves_caller_owned_connection(bool synchronous)
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        await database.Execute("CREATE TABLE business (id INT PRIMARY KEY) ENGINE=InnoDB");
        await using var provider = Services();
        await using var connection = await database.Source.OpenConnectionAsync();
        var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        var transaction = await outbox.BeginTransactionAsync(connection);
        await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: transaction);
        await outbox.EnlistAsync(Record(), default);
        if (synchronous) scope.Dispose(); else await scope.DisposeAsync();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(transaction.Connection).IsNull();
        await Assert.That(await Scalar(database, "SELECT count(*) FROM business")).IsEqualTo(0L);
        await Assert.That(await Scalar(database, "SELECT count(*) FROM relayer_outbox")).IsEqualTo(0L);
        await Assert.That(async () => await outbox.BeginTransactionAsync(connection)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Ambient_transactions_are_rejected_before_opening_connections()
    {
        await using var source = new MySqlDataSource("Server=invalid;User ID=test");
        await using var provider = Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<MySqlOutbox>();
        using var ambient = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled);
        await Assert.That(async () => await outbox.BeginTransactionAsync(source)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Configuration_binds_immutable_options_and_scopes_share_only_their_own_enlistment()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
            { ["Lanes"] = "8", ["TablePrefix"] = "custom_" }).Build();
        var services = new ServiceCollection();
        services.AddRelayerMySqlOutbox(configuration);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        await Assert.That(first.ServiceProvider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>().Value.Lanes).IsEqualTo(8);
        await Assert.That(first.ServiceProvider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>().Value.TablePrefix).IsEqualTo("custom_");
        await Assert.That(first.ServiceProvider.GetRequiredService<IOutboxEnlistment>()).IsSameReferenceAs(first.ServiceProvider.GetRequiredService<MySqlOutbox>());
        await Assert.That(first.ServiceProvider.GetRequiredService<IOutboxEnlistment>()).IsNotSameReferenceAs(second.ServiceProvider.GetRequiredService<IOutboxEnlistment>());
        configuration["Lanes"] = "16";
        configuration["TablePrefix"] = "next_";
        configuration.Reload();
        await using var reloaded = provider.CreateAsyncScope();
        await Assert.That(first.ServiceProvider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>().Value.Lanes).IsEqualTo(8);
        await Assert.That(reloaded.ServiceProvider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>().Value.Lanes).IsEqualTo(16);
        await Assert.That(reloaded.ServiceProvider.GetRequiredService<IOptionsSnapshot<MySqlOutboxOptions>>().Value.TablePrefix).IsEqualTo("next_");
    }

    [Test]
    [Arguments("Lanes", "0")]
    [Arguments("Lanes", "invalid")]
    [Arguments("TablePrefix", "this_prefix_is_far_too_long_for_mysql_identifier_names_and_must_fail_")]
    public async Task Invalid_configuration_fails_at_startup(string key, string value)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { [key] = value }).Build();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayerMySqlOutbox(configuration);
        using var host = builder.Build();
        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>();
    }

    private static ServiceProvider Services()
    {
        var services = new ServiceCollection();
        services.AddRelayerMySqlOutbox(new MySqlOutboxOptions { Lanes = 8 });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static OutboxRecord Record() => new(Guid.CreateVersion7(), "events", "order;v=1",
        new OutgoingMessage("orders", new byte[] { 1 }), DateTimeOffset.UtcNow);

    private static async Task<long> Scalar(MySqlFixture database, string sql)
    {
        await using var command = database.Source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
}
