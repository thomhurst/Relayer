using System.Buffers;
using System.Data;
using System.Data.Common;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public class AdoNetEnlistmentTests(SqlServerFixture fixture)
{
    [Test]
    public async Task Database_cleanup_disconnects_remaining_sessions()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var connection = await database.Open();
        // ClearPool marks checked-out connections for disposal; it does not close their server sessions.
        await database.DisposeAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Dapper_business_commands_and_outbox_rows_commit_atomically_in_one_batch(bool synchronous)
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using (var setup = await database.Open())
        {
            await setup.ExecuteAsync($"CREATE TABLE {database.BatchLog} (rows_inserted bigint NOT NULL)");
            await setup.ExecuteAsync($"CREATE TRIGGER [" + database.Options.Schema!.Replace("]", "]]") + "].[" + database.Options.TablePrefix +
                $"insert_log] ON {database.Table} AFTER INSERT AS INSERT INTO {database.BatchLog} SELECT COUNT_BIG(*) FROM inserted");
        }
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await using var connection = await database.Open();
        await using var transaction = await outbox.BeginTransactionAsync(connection, IsolationLevel.ReadCommitted);
        await Assert.That(transaction.IsolationLevel).IsEqualTo(IsolationLevel.ReadCommitted);
        await Assert.That(enlistment.IsActive).IsTrue();
        if (synchronous)
            transaction.Connection!.Execute($"INSERT INTO {database.Business} VALUES (@id)", new { id = 1 }, transaction);
        else
            await transaction.Connection!.ExecuteAsync($"INSERT INTO {database.Business} VALUES (@id)", new { id = 1 }, transaction);
        // Enough rows to show one native batch. The copy runs at roughly a thousand rows a second on a
        // busy shared server, so a larger count only eats into SqlBulkCopy's 30-second default timeout.
        for (var i = 0; i < 1_000; i++) await enlistment.EnlistAsync(Record(), default);
        await Assert.That(await database.Count()).IsEqualTo(0L);
        if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
        await Assert.That(enlistment.IsActive).IsFalse();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(await database.Count()).IsEqualTo(1_000L);
        await Assert.That(await database.Scalar($"SELECT count_big(*) FROM {database.BatchLog}")).IsEqualTo(1L);
        await Assert.That(await database.Scalar($"SELECT SUM(rows_inserted) FROM {database.BatchLog}")).IsEqualTo(1_000L);
        await Assert.That(await database.Scalar($"SELECT count_big(*) FROM {database.Business}")).IsEqualTo(1L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Rollback_and_disposal_discard_business_changes_and_buffered_rows(bool synchronous)
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await using var connection = await database.Open();
        await using (var transaction = await outbox.BeginTransactionAsync(connection))
        {
            await transaction.Connection!.ExecuteAsync($"INSERT INTO {database.Business} VALUES (1)", transaction: transaction);
            await enlistment.EnlistAsync(Record(), default);
            if (synchronous) transaction.Rollback(); else await transaction.RollbackAsync();
        }
        var abandoned = await outbox.BeginTransactionAsync(connection);
        await abandoned.Connection!.ExecuteAsync($"INSERT INTO {database.Business} VALUES (2)", transaction: abandoned);
        await enlistment.EnlistAsync(Record(), default);
        if (synchronous) abandoned.Dispose(); else await abandoned.DisposeAsync();
        await Assert.That(enlistment.IsActive).IsFalse();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await using (var next = await outbox.BeginTransactionAsync(connection)) await next.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar($"SELECT count_big(*) FROM {database.Business}")).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Insert_failure_rolls_back_complete_unit(bool synchronous)
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        var duplicate = Record();
        await using (var first = await outbox.BeginTransactionAsync(database.ConnectionString))
        {
            await enlistment.EnlistAsync(duplicate, default);
            await first.CommitAsync();
        }
        await using (var failedInsert = await outbox.BeginTransactionAsync(database.ConnectionString))
        {
            await failedInsert.Connection!.ExecuteAsync($"INSERT INTO {database.Business} VALUES (1)", transaction: failedInsert);
            await enlistment.EnlistAsync(duplicate, default);
            if (synchronous) await Assert.That(() => failedInsert.Commit()).Throws<SqlException>();
            else await Assert.That(async () => await failedInsert.CommitAsync()).Throws<SqlException>();
        }
        await Assert.That(enlistment.IsActive).IsFalse();
        await using (var next = await outbox.BeginTransactionAsync(database.ConnectionString)) await next.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar($"SELECT count_big(*) FROM {database.Business}")).IsEqualTo(0L);
    }

    [Test]
    public async Task Cancellation_aborts_commit_and_does_not_poison_next_transaction()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await outbox.BeginTransactionAsync(database.ConnectionString, cancellationToken: canceled.Token))
            .Throws<OperationCanceledException>();
        await using var connection = await database.Open();
        await using (var transaction = await outbox.BeginTransactionAsync(connection))
        {
            await transaction.Connection!.ExecuteAsync($"INSERT INTO {database.Business} VALUES (1)", transaction: transaction);
            await Assert.That(async () => await enlistment.EnlistAsync(Record(), canceled.Token)).Throws<OperationCanceledException>();
            await enlistment.EnlistAsync(Record(), default);
            await Assert.That(async () => await transaction.CommitAsync(canceled.Token)).Throws<OperationCanceledException>();
        }
        await Assert.That(enlistment.IsActive).IsFalse();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await using (var next = await outbox.BeginTransactionAsync(connection)) await next.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar($"SELECT count_big(*) FROM {database.Business}")).IsEqualTo(0L);
    }

    [Test]
    public async Task Enlistment_owns_borrowed_storage_and_preserves_original_metadata_for_store_reads()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        var id = Guid.CreateVersion7();
        var created = new DateTimeOffset(2026, 1, 1, 2, 3, 4, TimeSpan.FromHours(1));
        var delivery = created.AddDays(1);
        byte[] body = [0, 255, 1];
        byte[] key = System.Text.Encoding.UTF8.GetBytes("café");
        byte[] value = [0xff, 0, 0xfe];
        KeyValuePair<string, WireString>[] headers = [new("duplicate", new WireString("first")), new("duplicate", new WireString(value)), new("empty", new WireString(""))];
        await using (var transaction = await outbox.BeginTransactionAsync(database.ConnectionString))
        {
            var outgoing = new OutgoingMessage("original-destination", body, headers, new WireString(key), delivery);
            await enlistment.EnlistAsync(new(id, "original-transport", "stable.message;v=8", outgoing, created, TenantId: "tenant"), default);
            body.AsSpan().Fill(7);
            key.AsSpan().Fill(7);
            value.AsSpan().Fill(7);
            headers.AsSpan().Clear();
            await transaction.CommitAsync();
        }
        var lane = (int)await database.Scalar($"SELECT CAST(lane AS bigint) FROM {database.Table}");
        var store = new SqlServerOutboxStore(database.ConnectionString, database.Options);
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(lane, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(1);
        var row = rows[0];
        await Assert.That(row.Id).IsEqualTo(id);
        await Assert.That(row.MessageType).IsEqualTo("stable.message;v=8");
        await Assert.That(row.TransportName).IsEqualTo("original-transport");
        await Assert.That(row.CreatedAt).IsEqualTo(created.ToUniversalTime());
        await Assert.That(row.TenantId).IsEqualTo("tenant");
        await Assert.That(row.Message.Destination).IsEqualTo("original-destination");
        await Assert.That(row.Message.DeliverAt).IsEqualTo(delivery.ToUniversalTime());
        await Assert.That(row.Message.OrderingKey!.Value.ToString()).IsEqualTo("café");
        await Assert.That(row.Message.Body.ToArray()).IsEquivalentTo(new byte[] { 0, 255, 1 });
        await Assert.That(row.Message.Headers.Length).IsEqualTo(3);
        await Assert.That(row.Message.Headers.Span[0].Key).IsEqualTo("duplicate");
        await Assert.That(row.Message.Headers.Span[0].Value.ToString()).IsEqualTo("first");
        await Assert.That(row.Message.Headers.Span[1].Key).IsEqualTo("duplicate");
        await Assert.That(row.Message.Headers.Span[1].Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 0xff, 0, 0xfe });
        await Assert.That(row.Message.Headers.Span[2].Value.ToString()).IsEqualTo("");
        await Assert.That(lane is >= 0 and < 8).IsTrue();
    }

    [Test]
    public async Task Native_command_constraint_and_unsupported_savepoints_fail_explicitly()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        await using var closed = new SqlConnection(database.ConnectionString);
        await Assert.That(async () => await outbox.BeginTransactionAsync(closed)).Throws<InvalidOperationException>();
        await using var connection = await database.Open();
        await using var transaction = await outbox.BeginTransactionAsync(connection);
        await Assert.That(async () => await outbox.BeginTransactionAsync(database.ConnectionString)).Throws<InvalidOperationException>();
        using DbCommand native = connection.CreateCommand();
        await Assert.That(() => native.Transaction = transaction).Throws<InvalidCastException>();
        await Assert.That(transaction.SupportsSavepoints).IsFalse();
        await Assert.That(() => transaction.Save("checkpoint")).Throws<NotSupportedException>();
        await transaction.RollbackAsync();
    }

    [Test]
    public async Task Invalid_rows_and_native_callbacks_are_rejected_before_any_durable_write()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await Assert.That(async () => await enlistment.EnlistAsync(Record(), default)).Throws<InvalidOperationException>();
        await using var transaction = await outbox.BeginTransactionAsync(database.ConnectionString);
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { Id = Guid.NewGuid() }, default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { MessageType = "" }, default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { Attempts = 1 }, default)).Throws<ArgumentException>();
        var native = new OutgoingMessage("destination", new byte[] { 1 }, options: new SendOptions().ConfigureNative<object>(static _ => { }));
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { Message = native }, default)).Throws<InvalidOperationException>();
        await transaction.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
    }

    [Test]
    public async Task Registration_is_scoped_idempotent_and_rejects_competing_integrations()
    {
        var services = new ServiceCollection();
        services.AddRelayerSqlServerOutbox();
        services.AddRelayerSqlServerOutbox();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        await Assert.That(ReferenceEquals(first.ServiceProvider.GetRequiredService<SqlServerOutbox>(), first.ServiceProvider.GetRequiredService<IOutboxEnlistment>())).IsTrue();
        await Assert.That(ReferenceEquals(first.ServiceProvider.GetRequiredService<SqlServerOutbox>(), second.ServiceProvider.GetRequiredService<SqlServerOutbox>())).IsFalse();
        var competing = new ServiceCollection().AddScoped<IOutboxEnlistment>(_ => throw new InvalidOperationException());
        await Assert.That(() => competing.AddRelayerSqlServerOutbox()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Immutable_configuration_binds_all_options_and_reloads_only_for_new_scopes()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Schema"] = "configured", ["TablePrefix"] = "prefix_", ["Lanes"] = "8"
        }).Build();
        var services = new ServiceCollection().AddRelayerSqlServerOutbox(configuration);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var first = provider.CreateAsyncScope();
        var original = first.ServiceProvider.GetRequiredService<IOptionsSnapshot<SqlServerOutboxOptions>>().Value;
        await Assert.That(original.Schema).IsEqualTo("configured");
        await Assert.That(original.TablePrefix).IsEqualTo("prefix_");
        await Assert.That(original.Lanes).IsEqualTo(8);
        configuration["Lanes"] = "16";
        configuration.Reload();
        await using var second = provider.CreateAsyncScope();
        await Assert.That(second.ServiceProvider.GetRequiredService<IOptionsSnapshot<SqlServerOutboxOptions>>().Value.Lanes).IsEqualTo(16);
        await Assert.That(first.ServiceProvider.GetRequiredService<IOptionsSnapshot<SqlServerOutboxOptions>>().Value.Lanes).IsEqualTo(8);
        await Assert.That(first.ServiceProvider.GetRequiredService<IOutboxEnlistment>().IsActive).IsFalse();
    }

    [Test]
    [Arguments("Lanes", "0")]
    [Arguments("Lanes", "invalid")]
    [Arguments("TablePrefix", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")]
    public async Task Invalid_configuration_fails_at_host_startup(string key, string value)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { [key] = value }).Build();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayerSqlServerOutbox(configuration);
        using var host = builder.Build();
        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>();
    }

    [Test]
    public async Task Commands_reject_foreign_transactions_connection_closure_and_reuse_after_completion()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        await using var otherScope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var otherOutbox = otherScope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        await using var native = await database.Open();
        await using var transaction = await outbox.BeginTransactionAsync(native);
        var view = transaction.Connection!;
        await using var other = await otherOutbox.BeginTransactionAsync(database.ConnectionString);
        await using var command = view.CreateCommand();
        command.CommandText = "SELECT 42";
        await Assert.That(() => command.Transaction = other).Throws<InvalidOperationException>();
        await Assert.That(command.Connection).IsSameReferenceAs(native);
        command.Connection = native;
        await Assert.That(() => command.Connection = other.Connection).Throws<InvalidOperationException>();
        await Assert.That(() => view.Close()).Throws<InvalidOperationException>();
        await Assert.That(async () => await command.ExecuteReaderAsync(CommandBehavior.CloseConnection)).Throws<NotSupportedException>();
        await Assert.That(() => command.ExecuteReader(CommandBehavior.CloseConnection)).Throws<NotSupportedException>();
        // Disposing the borrowed connection view does not dispose the transaction's native connection.
        await view.DisposeAsync();
        command.Transaction = null;
        await Assert.That((int)(await command.ExecuteScalarAsync())!).IsEqualTo(42);
        await transaction.CommitAsync();
        await using var next = await outbox.BeginTransactionAsync(native);
        await Assert.That(async () => await command.ExecuteScalarAsync()).Throws<InvalidOperationException>();
        await Assert.That(() => command.Prepare()).Throws<InvalidOperationException>();
        await Assert.That(() => view.CreateCommand()).Throws<InvalidOperationException>();
        await next.RollbackAsync();
    }

    [Test]
    public async Task Sender_routes_active_sends_to_outbox_without_sending_before_commit()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var transport = provider.GetRequiredService<Transport>();
        await sender.SendAsync(new Probe(1));
        await Assert.That(transport.Sends).IsEqualTo(1);
        await using (var transaction = await outbox.BeginTransactionAsync(database.ConnectionString))
        {
            await sender.SendAsync(new Probe(2));
            await sender.SendViaOutboxAsync(new Probe(3));
            await Assert.That(transport.Sends).IsEqualTo(1);
            await Assert.That(await database.Count()).IsEqualTo(0L);
            await transaction.CommitAsync();
        }
        await Assert.That(await database.Count()).IsEqualTo(2L);
        await Assert.That(transport.Sends).IsEqualTo(1);
        var rows = new OutboxRecord[2];
        var store = new SqlServerOutboxStore(database.ConnectionString, database.Options);
        await Assert.That(await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(2);
        await Assert.That(rows[0].MessageType).IsEqualTo("ado.probe;v=1");
        var header = rows[0].Message.Headers.ToArray().Single(header => header.Key == "ce_id");
        await Assert.That(header.Value.ToString()).IsEqualTo(rows[0].Id.ToString("D"));
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Scope_disposal_abandons_owned_connection_and_completed_units_can_be_reused(bool synchronous)
    {
        await using var database = new Database(fixture);
        await database.Start();
        using var provider = database.Services();
        var scope = provider.CreateScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        var abandoned = await outbox.BeginTransactionAsync(database.ConnectionString);
        var view = abandoned.Connection!;
        await view.ExecuteAsync($"INSERT INTO {database.Business} VALUES (1)", transaction: abandoned);
        await outbox.EnlistAsync(Record(), default);
        if (synchronous) scope.Dispose(); else await ((IAsyncDisposable)scope).DisposeAsync();
        await Assert.That(view.State).IsEqualTo(ConnectionState.Closed);
        await abandoned.DisposeAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar($"SELECT count_big(*) FROM {database.Business}")).IsEqualTo(0L);
    }

    [Test]
    public async Task Cancellation_during_blocked_bulk_copy_rolls_back_complete_unit()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        await using var connection = await database.Open();
        await using var transaction = await outbox.BeginTransactionAsync(connection);
        var session = await transaction.Connection!.ExecuteScalarAsync<int>("SELECT @@SPID", transaction: transaction);
        await transaction.Connection!.ExecuteAsync($"INSERT INTO {database.Business} VALUES (1)", transaction: transaction);
        await outbox.EnlistAsync(Record(), default);
        await using var blocker = await database.Open();
        await using var blockingTransaction = (SqlTransaction)await blocker.BeginTransactionAsync();
        using (var block = new SqlCommand($"SELECT COUNT_BIG(*) FROM {database.Table} WITH (TABLOCKX, HOLDLOCK)", blocker, blockingTransaction))
            await block.ExecuteScalarAsync();
        using var cancellation = new CancellationTokenSource();
        var commit = transaction.CommitAsync(cancellation.Token);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await using var observer = await database.Open();
        while (await observer.ExecuteScalarAsync<int>(new CommandDefinition(
            "SELECT count(*) FROM sys.dm_exec_requests WHERE session_id=@session AND blocking_session_id > 0",
            new { session }, cancellationToken: deadline.Token)) == 0)
            await Task.Delay(10, deadline.Token);
        cancellation.Cancel();
        // SqlClient 7.1.1 does not interrupt every server-side bulk-copy wait. Release the observed
        // blocker so the native operation can finish; canceled commit must still roll back the whole unit.
        await blockingTransaction.RollbackAsync();
        await Assert.That(async () => await commit).Throws<OperationCanceledException>();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar($"SELECT count_big(*) FROM {database.Business}")).IsEqualTo(0L);
        await using var next = await outbox.BeginTransactionAsync(connection);
        await next.CommitAsync();
    }

    [Test]
    public async Task Ambient_transactions_and_scheduled_sender_calls_fail_explicitly()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        using (var ambient = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled))
            await Assert.That(async () => await outbox.BeginTransactionAsync(database.ConnectionString)).Throws<InvalidOperationException>();
        await using var transaction = await outbox.BeginTransactionAsync(database.ConnectionString);
        await Assert.That(async () => await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new Probe(1),
            options => options.DeliverAt = DateTimeOffset.UtcNow.AddDays(1))).Throws<InvalidOperationException>();
        await transaction.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
    }

    [Test]
    public async Task Large_streamed_payloads_then_small_empty_values_reuse_storage_without_trailing_bytes()
    {
        await using var database = new Database(fixture);
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<SqlServerOutbox>();
        foreach (var size in new[] { 100_000, 0, 3 })
        {
            var body = new byte[size];
            Array.Fill(body, (byte)255);
            var key = new string('x', size);
            KeyValuePair<string, WireString>[] headers = [new("large", new WireString(body))];
            await using (var transaction = await outbox.BeginTransactionAsync(database.ConnectionString))
            {
                await outbox.EnlistAsync(Record() with { Message = new OutgoingMessage("destination", body, headers, new WireString(key)) }, default);
                Array.Clear(body);
                await transaction.CommitAsync();
            }
            var lane = await database.Scalar($"SELECT CAST(lane AS bigint) FROM {database.Table}");
            var rows = new OutboxRecord[1];
            var store = new SqlServerOutboxStore(database.ConnectionString, database.Options);
            await Assert.That(await store.ReadPendingAsync(new((int)lane, DateTimeOffset.UtcNow), rows, default)).IsEqualTo(1);
            await Assert.That(rows[0].Message.Body.Length).IsEqualTo(size);
            await Assert.That(rows[0].Message.Body.ToArray().All(value => value == 255)).IsTrue();
            await Assert.That(rows[0].Message.Headers.Span[0].Value.Utf8.Length).IsEqualTo(size);
            await Assert.That(rows[0].Message.OrderingKey!.Value.ToString()).IsEqualTo(key);
            await using var cleanup = await database.Open();
            await cleanup.ExecuteAsync($"DELETE FROM {database.Table}");
        }
    }

    private static OutboxRecord Record() => new(Guid.CreateVersion7(), "events", "ado.probe;v=1",
        new OutgoingMessage("destination", new byte[] { 1 }), DateTimeOffset.UtcNow);

    private sealed class Database(SqlServerFixture fixture) : IAsyncDisposable
    {
        private readonly string _name = TestContext.Current!.Isolation.GetIsolatedName("ado");
        private bool _created;
        internal SqlServerOutboxOptions Options { get; } = new() { Schema = "ADO]" + Guid.NewGuid().ToString("N"), TablePrefix = "t_" + Guid.NewGuid().ToString("N") + "_", Lanes = 8 };
        internal string Table => "[" + Options.Schema!.Replace("]", "]]") + "].[" + Options.TablePrefix + "outbox]";
        internal string BatchLog => "[" + Options.Schema!.Replace("]", "]]") + "].[" + Options.TablePrefix + "batch_log]";
        internal string Business => "[" + Options.Schema!.Replace("]", "]]") + "].[" + Options.TablePrefix + "business]";
        internal string ConnectionString => new SqlConnectionStringBuilder(fixture.ConnectionString) { InitialCatalog = _name }.ConnectionString;
        internal async Task Start()
        {
            // Bulk copy and concurrent schema creation must not contend in the shared master database.
            await using (var admin = new SqlConnection(fixture.ConnectionString))
            {
                await admin.OpenAsync();
                await using var create = new SqlCommand($"CREATE DATABASE [{_name.Replace("]", "]]")}]", admin);
                await create.ExecuteNonQueryAsync();
                _created = true;
            }
            await using var connection = await Open();
            await using var command = new SqlCommand(SqlServerOutboxSchema.CreateScript(Options) +
                $"CREATE TABLE {Business} (id int PRIMARY KEY);", connection);
            await command.ExecuteNonQueryAsync();
        }
        internal async Task<SqlConnection> Open()
        {
            var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();
            return connection;
        }
        internal ServiceProvider Services()
        {
            var services = new ServiceCollection();
            services.AddRelayerSqlServerOutbox(Options);
            services.AddSingleton<Transport>();
            services.AddSingleton<ITransport>(static provider => provider.GetRequiredService<Transport>());
            services.AddSingleton<IMessageSerializer, Serializer>();
            services.AddRelayer(builder =>
            {
                builder.MessageTypes.Add<Probe>("ado.probe");
                builder.AddTransport<Empty, Empty, Empty>("events", transport => transport.Send<Probe>("destination"));
            });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }
        internal Task<long> Count() => Scalar($"SELECT count_big(*) FROM {Table}");
        internal async Task<long> Scalar(string sql)
        {
            await using var connection = await Open();
            await using var command = new SqlCommand(sql, connection);
            return (long)(await command.ExecuteScalarAsync())!;
        }
        public async ValueTask DisposeAsync()
        {
            if (!_created) return;
            using (var pool = new SqlConnection(ConnectionString)) SqlConnection.ClearPool(pool);
            await using var admin = new SqlConnection(fixture.ConnectionString);
            await admin.OpenAsync();
            var identifier = "[" + _name.Replace("]", "]]") + "]";
            // End remaining server sessions before dropping this test's isolated database.
            await using var drop = new SqlCommand($"ALTER DATABASE {identifier} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {identifier}", admin);
            await drop.ExecuteNonQueryAsync();
            _created = false;
        }
    }

    [Message("ado.probe")]
    public sealed record Probe(byte Value);
    public sealed class Empty;
    private sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "application/test";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination)
        {
            destination.GetSpan(1)[0] = ((Probe)(object)message!).Value;
            destination.Advance(1);
        }
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => throw new NotSupportedException();
    }
    private sealed class Transport : ITransport
    {
        public int Sends { get; private set; }
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken) { Sends++; return default; }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => throw new NotSupportedException();
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
