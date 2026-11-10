using System.Buffers;
using System.Data;
using System.Data.Common;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class AdoNetEnlistmentTests
{
    [Test]
    public async Task Batch_assigns_insert_sequence_in_enqueue_order_instead_of_UUID_order()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        await using var transaction = await outbox.BeginTransactionAsync(database.Source);
        var first = Guid.Parse("01900000-0000-7000-8000-000000000002");
        var second = Guid.Parse("01900000-0000-7000-8000-000000000001");
        await outbox.EnlistAsync(Record() with { Id = first }, default);
        await outbox.EnlistAsync(Record() with { Id = second }, default);
        await transaction.CommitAsync();
        await using var connection = await database.Source.OpenConnectionAsync();
        var ids = await connection.QueryAsync<Guid>($"SELECT id FROM {Database.Table} ORDER BY seq");
        await Assert.That(ids.SequenceEqual([first, second])).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Dapper_preserves_postgresql_array_parameters_in_business_commands(bool synchronous)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        await using var transaction = await outbox.BeginTransactionAsync(database.Source);
        await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1), (2)", transaction: transaction);
        const string sql = "SELECT count(*)::integer FROM business WHERE id = ANY(@Ids)";
        var parameters = new { Ids = new[] { 1, 2 } };
        var count = synchronous
            ? transaction.Connection!.ExecuteScalar<int>(sql, parameters, transaction)
            : await transaction.Connection!.ExecuteScalarAsync<int>(sql, parameters, transaction);
        await Assert.That(count).IsEqualTo(2);
        var empty = new { Ids = Array.Empty<int>() };
        var emptyCount = synchronous
            ? transaction.Connection!.ExecuteScalar<int>(sql, empty, transaction)
            : await transaction.Connection!.ExecuteScalarAsync<int>(sql, empty, transaction);
        await Assert.That(emptyCount).IsEqualTo(0);
        await outbox.EnlistAsync(Record(), default);
        await transaction.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(2L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Dapper_business_commands_and_outbox_rows_commit_atomically_in_one_batch(bool synchronous)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await outbox.BeginTransactionAsync(connection, IsolationLevel.ReadCommitted);
        await Assert.That(transaction.IsolationLevel).IsEqualTo(IsolationLevel.ReadCommitted);
        await Assert.That(enlistment.IsActive).IsTrue();
        if (synchronous)
            transaction.Connection!.Execute("INSERT INTO business VALUES (@id)", new { id = 1 }, transaction);
        else
            await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (@id)", new { id = 1 }, transaction);
        for (var i = 0; i < 7_000; i++) await enlistment.EnlistAsync(Record(), default);
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(0L);
        if (synchronous) transaction.Commit(); else await transaction.CommitAsync();
        await Assert.That(enlistment.IsActive).IsFalse();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(await database.Count()).IsEqualTo(7_000L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM batch_log")).IsEqualTo(1L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Rollback_and_disposal_discard_business_changes_and_buffered_rows(bool synchronous)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using (var transaction = await outbox.BeginTransactionAsync(connection))
        {
            await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: transaction);
            await enlistment.EnlistAsync(Record(), default);
            if (synchronous) transaction.Rollback(); else await transaction.RollbackAsync();
        }
        var abandoned = await outbox.BeginTransactionAsync(connection);
        await abandoned.Connection!.ExecuteAsync("INSERT INTO business VALUES (2)", transaction: abandoned);
        await enlistment.EnlistAsync(Record(), default);
        if (synchronous) abandoned.Dispose(); else await abandoned.DisposeAsync();
        await Assert.That(enlistment.IsActive).IsFalse();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await using (var next = await outbox.BeginTransactionAsync(connection)) await next.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Insert_failure_and_deferred_commit_failure_roll_back_complete_unit(bool synchronous)
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        var duplicate = Record();
        await using (var first = await outbox.BeginTransactionAsync(database.Source))
        {
            await enlistment.EnlistAsync(duplicate, default);
            await first.CommitAsync();
        }
        await using (var failedInsert = await outbox.BeginTransactionAsync(database.Source))
        {
            await failedInsert.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: failedInsert);
            await enlistment.EnlistAsync(duplicate, default);
            if (synchronous) await Assert.That(() => failedInsert.Commit()).Throws<PostgresException>();
            else await Assert.That(async () => await failedInsert.CommitAsync()).Throws<PostgresException>();
        }
        await using (var failedCommit = await outbox.BeginTransactionAsync(database.Source))
        {
            await failedCommit.Connection!.ExecuteAsync("INSERT INTO deferred_business VALUES (1, 999)", transaction: failedCommit);
            await enlistment.EnlistAsync(Record(), default);
            if (synchronous) await Assert.That(() => failedCommit.Commit()).Throws<PostgresException>();
            else await Assert.That(async () => await failedCommit.CommitAsync()).Throws<PostgresException>();
        }
        await Assert.That(enlistment.IsActive).IsFalse();
        await using (var next = await outbox.BeginTransactionAsync(database.Source)) await next.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM deferred_business")).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM batch_log")).IsEqualTo(1L);
    }

    [Test]
    public async Task Cancellation_aborts_commit_and_does_not_poison_next_transaction()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await outbox.BeginTransactionAsync(database.Source, cancellationToken: canceled.Token))
            .Throws<OperationCanceledException>();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using (var transaction = await outbox.BeginTransactionAsync(connection))
        {
            await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: transaction);
            await Assert.That(async () => await enlistment.EnlistAsync(Record(), canceled.Token)).Throws<OperationCanceledException>();
            await enlistment.EnlistAsync(Record(), default);
            await Assert.That(async () => await transaction.CommitAsync(canceled.Token)).Throws<OperationCanceledException>();
        }
        await Assert.That(enlistment.IsActive).IsFalse();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await using (var next = await outbox.BeginTransactionAsync(connection)) await next.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(0L);
    }

    [Test]
    public async Task Enlistment_owns_borrowed_storage_and_preserves_original_metadata_for_store_reads()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        var id = Guid.CreateVersion7();
        var created = new DateTimeOffset(2026, 1, 1, 2, 3, 4, TimeSpan.FromHours(1));
        var delivery = created.AddDays(1);
        byte[] body = [0, 255, 1];
        byte[] key = System.Text.Encoding.UTF8.GetBytes("café");
        byte[] value = [0xff, 0, 0xfe];
        KeyValuePair<string, WireString>[] headers = [new("duplicate", new WireString("first")), new("duplicate", new WireString(value)), new("empty", new WireString(""))];
        await using (var transaction = await outbox.BeginTransactionAsync(database.Source))
        {
            var outgoing = new OutgoingMessage("original-destination", body, headers, new WireString(key), delivery);
            await enlistment.EnlistAsync(new(id, "original-transport", "stable.message;v=8", outgoing, created, TenantId: "tenant"), default);
            body.AsSpan().Fill(7);
            key.AsSpan().Fill(7);
            value.AsSpan().Fill(7);
            headers.AsSpan().Clear();
            await transaction.CommitAsync();
        }
        var lane = (int)await database.Scalar($"SELECT lane::bigint FROM {Database.Table}");
        var store = new PostgreSqlOutboxStore(database.Source, Database.Options);
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
    public async Task Data_source_connection_is_owned_and_scope_disposal_abandons_active_unit()
    {
        await using var database = new Database();
        await database.Start();
        // An unrelated connection must stay usable while each owned connection closes.
        await using var unrelated = await database.Source.OpenConnectionAsync();
        await using var provider = database.Services();
        DbTransaction abandoned;
        int abandonedBackend;
        await using (var scope = provider.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
            abandoned = await outbox.BeginTransactionAsync(database.Source);
            abandonedBackend = await abandoned.Connection!.QuerySingleAsync<int>("SELECT pg_backend_pid()", transaction: abandoned);
            await abandoned.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: abandoned);
            await scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().EnlistAsync(Record(), default);
            await Assert.That(await database.Scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'"))
                .IsEqualTo(1L);
        }
        await database.WaitForBackendExit(abandonedBackend);
        await abandoned.DisposeAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'"))
            .IsEqualTo(0L);
        await using (var nextScope = provider.CreateAsyncScope())
        {
            var outbox = nextScope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
            await using (var committed = await outbox.BeginTransactionAsync(database.Source))
            {
                var backend = await committed.Connection!.QuerySingleAsync<int>("SELECT pg_backend_pid()", transaction: committed);
                await committed.CommitAsync();
                await database.WaitForBackendExit(backend);
            }
            await using (var rolledBack = await outbox.BeginTransactionAsync(database.Source))
            {
                var backend = await rolledBack.Connection!.QuerySingleAsync<int>("SELECT pg_backend_pid()", transaction: rolledBack);
                await rolledBack.RollbackAsync();
                await database.WaitForBackendExit(backend);
            }
        }
        await using var usableSource = await database.Source.OpenConnectionAsync();
        await Assert.That(usableSource.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(await unrelated.ExecuteScalarAsync<int>("SELECT 42")).IsEqualTo(42);
    }

    [Test]
    public async Task Native_command_constraint_and_unsupported_savepoints_fail_explicitly()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        await using var closed = new NpgsqlConnection(database.ConnectionString);
        await Assert.That(async () => await outbox.BeginTransactionAsync(closed)).Throws<InvalidOperationException>();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await outbox.BeginTransactionAsync(connection);
        await Assert.That(async () => await outbox.BeginTransactionAsync(database.Source)).Throws<InvalidOperationException>();
        using DbCommand native = connection.CreateCommand();
        await Assert.That(() => native.Transaction = transaction).Throws<InvalidCastException>();
        await Assert.That(transaction.SupportsSavepoints).IsFalse();
        await Assert.That(() => transaction.Save("checkpoint")).Throws<NotSupportedException>();
        await transaction.RollbackAsync();
    }

    [Test]
    public async Task Invalid_rows_and_native_callbacks_are_rejected_before_any_durable_write()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var enlistment = scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>();
        await Assert.That(async () => await enlistment.EnlistAsync(Record(), default)).Throws<InvalidOperationException>();
        await using var transaction = await outbox.BeginTransactionAsync(database.Source);
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { Id = Guid.NewGuid() }, default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { MessageType = "" }, default)).Throws<InvalidOperationException>();
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { Attempts = 1 }, default)).Throws<ArgumentException>();
        var native = new OutgoingMessage("destination", new byte[] { 1 }, options: new SendOptions().ConfigureNative<object>(static _ => { }));
        await Assert.That(async () => await enlistment.EnlistAsync(Record() with { Message = native }, default)).Throws<InvalidOperationException>();
        await transaction.CommitAsync();
        await Assert.That(await database.Count()).IsEqualTo(0L);
    }

    [Test]
    public async Task Sender_routes_active_sends_to_outbox_without_sending_before_commit()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var sender = scope.ServiceProvider.GetRequiredService<ISender>();
        var transport = provider.GetRequiredService<Transport>();
        await sender.SendAsync(new Probe(1));
        await Assert.That(transport.Sends).IsEqualTo(1);
        await using (var transaction = await outbox.BeginTransactionAsync(database.Source))
        {
            await sender.SendAsync(new Probe(2));
            await sender.SendViaOutboxAsync(new Probe(3));
            await Assert.That(transport.Sends).IsEqualTo(1);
            await Assert.That(await database.Count()).IsEqualTo(0L);
            await transaction.CommitAsync();
        }
        await Assert.That(await database.Count()).IsEqualTo(2L);
        await Assert.That(transport.Sends).IsEqualTo(1);
        await using var command = database.Source.CreateCommand($"SELECT id, message_type, headers FROM {Database.Table} LIMIT 1");
        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        await Assert.That(reader.GetString(1)).IsEqualTo("ado.probe;v=1");
        KeyValuePair<string, WireString>[] entries = [];
        var headers = OutboxHeaderCodec.Read(reader.GetFieldValue<byte[]>(2), ref entries);
        await Assert.That(headers.ToArray().Single(header => header.Key == "ce_id").Value.ToString()).IsEqualTo(reader.GetGuid(0).ToString("D"));
    }

    [Test]
    public async Task Registration_is_scoped_idempotent_and_rejects_competing_integrations()
    {
        var services = new ServiceCollection();
        services.AddRelayerPostgreSqlOutbox();
        services.AddRelayerPostgreSqlOutbox();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        await Assert.That(ReferenceEquals(first.ServiceProvider.GetRequiredService<PostgreSqlOutbox>(), first.ServiceProvider.GetRequiredService<IOutboxEnlistment>())).IsTrue();
        await Assert.That(ReferenceEquals(first.ServiceProvider.GetRequiredService<PostgreSqlOutbox>(), second.ServiceProvider.GetRequiredService<PostgreSqlOutbox>())).IsFalse();
        await Assert.That(() => new ServiceCollection().AddRelayerEntityFrameworkCoreOutbox().AddRelayerPostgreSqlOutbox()).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceCollection().AddRelayerPostgreSqlOutbox().AddRelayerEntityFrameworkCoreOutbox()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Immutable_configuration_binds_all_options_and_reloads_only_for_new_scopes()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Schema"] = "configured", ["TablePrefix"] = "prefix_", ["Lanes"] = "8",
            ["LeaseDuration"] = "00:00:45", ["MaxDispatchAttempts"] = "7"
        }).Build();
        var services = new ServiceCollection().AddRelayerPostgreSqlOutbox(configuration);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var first = provider.CreateAsyncScope();
        var original = first.ServiceProvider.GetRequiredService<IOptionsSnapshot<PostgreSqlOutboxOptions>>().Value;
        await Assert.That(original.Schema).IsEqualTo("configured");
        await Assert.That(original.TablePrefix).IsEqualTo("prefix_");
        await Assert.That(original.Lanes).IsEqualTo(8);
        await Assert.That(original.LeaseDuration).IsEqualTo(TimeSpan.FromSeconds(45));
        await Assert.That(original.MaxDispatchAttempts).IsEqualTo(7);
        configuration["Lanes"] = "16";
        configuration.Reload();
        await using var second = provider.CreateAsyncScope();
        await Assert.That(second.ServiceProvider.GetRequiredService<IOptionsSnapshot<PostgreSqlOutboxOptions>>().Value.Lanes).IsEqualTo(16);
        await Assert.That(first.ServiceProvider.GetRequiredService<IOptionsSnapshot<PostgreSqlOutboxOptions>>().Value.Lanes).IsEqualTo(8);
        await Assert.That(first.ServiceProvider.GetRequiredService<IOutboxEnlistment>().IsActive).IsFalse();
    }

    [Test]
    [Arguments("Lanes", "0")]
    [Arguments("Lanes", "invalid")]
    [Arguments("TablePrefix", "this_prefix_is_far_too_long_for_postgresql_identifier_names_and_must_fail_")]
    public async Task Invalid_configuration_fails_at_host_startup(string key, string value)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { [key] = value }).Build();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRelayerPostgreSqlOutbox(configuration);
        using var host = builder.Build();
        await Assert.That(async () => await host.StartAsync()).Throws<OptionsValidationException>();
    }

    [Test]
    public async Task Commands_reject_foreign_transactions_connection_closure_and_reuse_after_completion()
    {
        await using var database = new Database();
        await database.Start();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        await using var otherScope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        var otherOutbox = otherScope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        await using var native = await database.Source.OpenConnectionAsync();
        await using var transaction = await outbox.BeginTransactionAsync(native);
        var view = transaction.Connection!;
        await using var other = await otherOutbox.BeginTransactionAsync(database.Source);
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
    public async Task Cancellation_during_blocked_insert_rolls_back_business_changes_and_releases_connection()
    {
        await using var database = new Database();
        await database.Start();
        await using (var install = database.Source.CreateCommand($"""
            CREATE FUNCTION block_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(118); RETURN NULL; END; $$;
            CREATE TRIGGER block_insert BEFORE INSERT ON {Database.Table} FOR EACH STATEMENT EXECUTE FUNCTION block_insert();
            """)) await install.ExecuteNonQueryAsync();
        await using var blocker = await database.Source.OpenConnectionAsync();
        await using (var hold = new NpgsqlCommand("SELECT pg_advisory_lock(118)", blocker)) await hold.ExecuteNonQueryAsync();
        await using var provider = database.Services();
        await using var scope = provider.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<PostgreSqlOutbox>();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await outbox.BeginTransactionAsync(connection);
        await transaction.Connection!.ExecuteAsync("INSERT INTO business VALUES (1)", transaction: transaction);
        await scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().EnlistAsync(Record(), default);
        using var cancellation = new CancellationTokenSource();
        var commit = transaction.CommitAsync(cancellation.Token);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        // Cancel only after PostgreSQL confirms the insert is waiting on our held lock.
        while (await database.Scalar("SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND datname = current_database()") == 0)
            await Task.Delay(10, timeout.Token);
        cancellation.Cancel();
        await Assert.That(async () => await commit).Throws<OperationCanceledException>();
        await Assert.That(connection.State).IsEqualTo(ConnectionState.Open);
        await Assert.That(scope.ServiceProvider.GetRequiredService<IOutboxEnlistment>().IsActive).IsFalse();
        await Assert.That(await database.Count()).IsEqualTo(0L);
        await Assert.That(await database.Scalar("SELECT count(*) FROM business")).IsEqualTo(0L);
        await using var next = await outbox.BeginTransactionAsync(connection);
        await next.CommitAsync();
    }

    private static OutboxRecord Record() => new(Guid.CreateVersion7(), "events", "ado.probe;v=1",
        new OutgoingMessage("destination", new byte[] { 1 }), DateTimeOffset.UtcNow);

    private sealed class Database : IAsyncDisposable
    {
        internal static PostgreSqlOutboxOptions Options => new() { Schema = "ADO\"Schema", TablePrefix = "a.\"_", Lanes = 8 };
        internal const string Table = "\"ADO\"\"Schema\".\"a.\"\"_outbox\"";
        private readonly PostgreSqlFixture _fixture = new();
        internal string ConnectionString => _fixture.ConnectionString;
        internal NpgsqlDataSource Source { get; private set; } = null!;
        internal async Task Start()
        {
            await _fixture.StartAsync();
            Source = NpgsqlDataSource.Create(ConnectionString);
            await using var command = Source.CreateCommand(PostgreSqlOutboxSchema.CreateScript(Options) + $"""
                CREATE TABLE business (id integer PRIMARY KEY);
                CREATE TABLE deferred_business (id integer PRIMARY KEY, parent integer REFERENCES business(id) DEFERRABLE INITIALLY DEFERRED);
                CREATE TABLE batch_log (id integer);
                CREATE FUNCTION log_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO batch_log VALUES (1); RETURN NULL; END; $$;
                CREATE TRIGGER batch_count AFTER INSERT ON {Table} FOR EACH STATEMENT EXECUTE FUNCTION log_batch();
                """);
            await command.ExecuteNonQueryAsync();
        }
        internal ServiceProvider Services()
        {
            var services = new ServiceCollection();
            services.AddRelayerPostgreSqlOutbox(Options);
            services.AddRelayerPostgreSqlOrderedDispatch("schedule", Source,
                new PostgreSqlOrderedDispatchOptions { Schema = Options.Schema, TablePrefix = Options.TablePrefix, Lanes = Options.Lanes });
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
        internal Task<long> Count() => Scalar($"SELECT count(*) FROM {Table}");
        internal async Task WaitForBackendExit(int backend)
        {
            // Client disposal sends Terminate without waiting for the server backend to exit.
            // Observe this connection only, not short-lived connections used by other assertions.
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await using var observer = await Source.OpenConnectionAsync(timeout.Token);
            await using var command = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE pid=$1", observer);
            command.Parameters.AddWithValue(backend);
            while ((long)(await command.ExecuteScalarAsync(timeout.Token))! != 0)
                await Task.Delay(10, timeout.Token);
        }
        internal async Task<long> Scalar(string sql)
        {
            await using var command = Source.CreateCommand(sql);
            return (long)(await command.ExecuteScalarAsync())!;
        }
        public async ValueTask DisposeAsync()
        {
            if (Source is not null) await Source.DisposeAsync();
            await _fixture.DisposeAsync();
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
