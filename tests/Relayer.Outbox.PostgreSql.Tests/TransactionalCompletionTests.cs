using Npgsql;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.PostgreSql.Tests;

public class TransactionalCompletionTests
{
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Completion_and_business_work_share_the_callers_commit_or_rollback(bool commit)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "local-work", TablePrefix = "custom_" };
        await PostgreSqlOutboxStoreTests.CreateSchema(source, options);
        await using (var create = source.CreateCommand("CREATE TABLE business_work (id integer PRIMARY KEY)"))
            await create.ExecuteNonQueryAsync();
        var store = new PostgreSqlOutboxStore(source, options);
        var now = DateTimeOffset.UtcNow;
        var claim = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        await PostgreSqlOutboxStoreTests.Insert(source, options, claim.MessageId, now);
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddHours(1), default)).IsTrue();

        await using var connection = await source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var business = new NpgsqlCommand("INSERT INTO business_work VALUES (1)", connection, transaction))
            await business.ExecuteNonQueryAsync();
        await Assert.That(await store.CompleteAsync(claim, transaction, default)).IsTrue();
        // A separate connection sees neither uncommitted business work nor an uncommitted deletion.
        await Assert.That(await Count(source, "business_work")).IsEqualTo(0L);
        await Assert.That(await Count(source, "\"local-work\".custom_outbox")).IsEqualTo(1L);
        await Assert.That(ReferenceEquals(transaction.Connection, connection)).IsTrue();
        if (commit) await transaction.CommitAsync();
        else await transaction.RollbackAsync();
        await Assert.That(await Count(source, "business_work")).IsEqualTo(commit ? 1L : 0L);
        await Assert.That(await Count(source, "\"local-work\".custom_outbox")).IsEqualTo(commit ? 0L : 1L);
        // Explicit transaction use must not stick to this scoped store.
        await Assert.That(await store.GetPendingCountAsync(now.AddDays(1), default)).IsEqualTo(commit ? 0L : 1L);
    }

    [Test]
    [Arguments("stale")]
    [Arguments("expired")]
    [Arguments("missing")]
    public async Task Lost_fences_allow_business_work_to_be_rolled_back(string kind)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var store = new PostgreSqlOutboxStore(source);
        var now = DateTimeOffset.UtcNow;
        var claim = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        if (kind != "missing")
        {
            var start = kind == "expired" ? now.AddHours(-2) : now;
            await PostgreSqlOutboxStoreTests.Insert(source, new(), claim.MessageId, start);
            await Assert.That(await store.TryClaimAsync(claim, start, start.AddHours(1), default)).IsTrue();
            if (kind == "stale")
            {
                // A different, live owner has already taken over the row.
                var replacement = new OutboxClaim(claim.MessageId, Guid.NewGuid());
                await Assert.That(await store.TryClaimAsync(replacement, now.AddHours(2), now.AddHours(3), default)).IsTrue();
            }
        }
        await using var connection = await source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var business = new NpgsqlCommand("CREATE TABLE business_work (id integer)", connection, transaction))
            await business.ExecuteNonQueryAsync();
        await Assert.That(await store.CompleteAsync(claim, transaction, default)).IsFalse();
        await transaction.RollbackAsync();
        await using var check = source.CreateCommand("SELECT to_regclass('business_work') IS NULL");
        await Assert.That((bool)(await check.ExecuteScalarAsync())!).IsTrue();
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(kind == "missing" ? 0L : 1L);
    }

    [Test]
    public async Task Cancellation_and_validation_leave_transaction_ownership_with_the_caller()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var store = new PostgreSqlOutboxStore(source);
        await using var connection = await source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        var claim = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await store.CompleteAsync(new(Guid.Empty, claim.Token), transaction, default));
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await store.CompleteAsync(claim, (NpgsqlTransaction)null!, default));
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await store.CompleteAsync(claim, transaction, cancellation.Token));
        await Assert.That(ReferenceEquals(transaction.Connection, connection)).IsTrue();
        await transaction.RollbackAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.CompleteAsync(claim, transaction, default));
        await using var another = await connection.BeginTransactionAsync();
        await Assert.That(await store.CompleteAsync(claim, another, default)).IsFalse();
        await another.CommitAsync();
    }

    [Test]
    public async Task Expiry_uses_wall_clock_after_transaction_start_and_supplied_database()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        // Completion must use the supplied transaction, never open the store's configured database.
        var unavailable = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { Database = "does_not_exist" };
        await using var otherSource = NpgsqlDataSource.Create(unavailable.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var claims = new PostgreSqlOutboxStore(source);
        var store = new PostgreSqlOutboxStore(otherSource);
        var now = DateTimeOffset.UtcNow;
        var claim = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        await PostgreSqlOutboxStoreTests.Insert(source, new(), claim.MessageId, now);
        await Assert.That(await claims.TryClaimAsync(claim, now, now.AddHours(1), default)).IsTrue();
        await using var connection = await source.OpenConnectionAsync();
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            // Wall clock is later than transaction_timestamp(), without sleeps or client clock assumptions.
            await using var expire = new NpgsqlCommand("UPDATE relayer_outbox SET claim_expires_at = clock_timestamp()", connection, transaction);
            await expire.ExecuteNonQueryAsync();
            await Assert.That(await store.CompleteAsync(claim, transaction, default)).IsFalse();
            await transaction.RollbackAsync();
        }
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await Assert.That(await store.CompleteAsync(claim, transaction, default)).IsTrue();
            // Disposal must restore the row without the store retaining the transaction.
        }
        await Assert.That(await Count(source, "relayer_outbox")).IsEqualTo(1L);
    }

    private static async Task<long> Count(NpgsqlDataSource source, string table)
    {
        await using var command = source.CreateCommand($"SELECT count(*) FROM {table}");
        return (long)(await command.ExecuteScalarAsync())!;
    }
}
