using Relayer.Transports;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using NpgsqlTypes;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.PostgreSql.Tests;

public class PostgreSqlOutboxStoreTests
{
    [Test]
    public async Task Reads_are_bounded_ordered_due_and_lossless_with_custom_identifiers()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "Order\";--", TablePrefix = "a.\"_" };
        await CreateSchema(source, options);
        var store = new PostgreSqlOutboxStore(source, options);
        var now = DateTimeOffset.UtcNow.AddSeconds(-1);
        var first = Guid.Parse("01900000-0000-7000-8000-000000000001");
        var second = Guid.Parse("01900000-0000-7000-8000-000000000002");
        var third = Guid.Parse("01900000-0000-7000-8000-000000000003");
        await Insert(source, options, third, now);
        await Insert(source, options, first, now, deliverAt: now);
        await Insert(source, options, second, now);
        await Insert(source, options, Guid.CreateVersion7(), now, lane: 3);
        await Insert(source, options, Guid.CreateVersion7(), now, deliverAt: now.AddSeconds(1));
        await Insert(source, options, Guid.CreateVersion7(), now, status: "parked");
        await Insert(source, options, Guid.CreateVersion7(), now, expires: now.AddSeconds(1));
        await Assert.That(await store.GetPendingCountAsync(now, default)).IsEqualTo(4L);

        var rows = new OutboxRecord[2];
        await Assert.That(await store.ReadPendingAsync(new(null, now), rows, default)).IsEqualTo(2);
        await Assert.That(rows[0].Id).IsEqualTo(first);
        await Assert.That(rows[1].Id).IsEqualTo(second);
        await Assert.That(rows[0].TransportName).IsEqualTo("events");
        await Assert.That(rows[0].MessageType).IsEqualTo("order.placed.v1");
        await Assert.That(rows[0].Message.Destination).IsEqualTo("orders");
        await Assert.That(rows[0].Message.OrderingKey!.Value.ToString()).IsEqualTo("customer-é");
        await Assert.That(rows[0].Message.Body.ToArray()).IsEquivalentTo(new byte[] { 0, 255, 1 });
        await Assert.That(rows[0].InsertSequence).IsEqualTo(2L);
        await Assert.That(rows[0].Message.Headers.Length).IsEqualTo(3);
        await Assert.That(rows[0].Message.Headers.Span[0].Key).IsEqualTo("duplicate");
        await Assert.That(rows[0].Message.Headers.Span[1].Key).IsEqualTo("duplicate");
        await Assert.That(rows[0].Message.Headers.Span[1].Value.Utf8.ToArray()).IsEquivalentTo(new byte[] { 0xff, 0, 0xfe });
        await Assert.That(rows[0].Message.Headers.Span[2].Value.IsEmpty).IsTrue();
        await Assert.That(rows[0].TenantId).IsEqualTo("tenant");
        await Assert.That(rows[0].CreatedAt).IsEqualTo(Microseconds(now));
        await Assert.That(rows[0].Message.DeliverAt).IsEqualTo(Microseconds(now));
        await Assert.That(await store.ReadPendingAsync(new(null, now, second), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(third);
        await Assert.That(await store.ReadPendingAsync(new(3, now), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Lane).IsEqualTo(3);
        await Assert.That(rows[0].InsertSequence).IsEqualTo(4L);
        await Assert.That(rows[0].Message.Headers.Length).IsEqualTo(3);
        await Assert.That(await store.ReadPendingAsync(new(4, now), rows, default)).IsEqualTo(0);
        await Assert.That(await store.ReadPendingAsync(new(null, now), Memory<OutboxRecord>.Empty, default)).IsEqualTo(0);
    }

    [Test]
    public async Task Competing_claims_are_atomic_and_expired_owners_cannot_mutate()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await CreateSchema(source);
        var a = new PostgreSqlOutboxStore(source);
        var b = new PostgreSqlOutboxStore(source);
        var now = Microseconds(DateTimeOffset.UtcNow);
        var id = Guid.CreateVersion7();
        await Insert(source, new(), id, now);
        var first = new OutboxClaim(id, Guid.NewGuid());
        var second = new OutboxClaim(id, Guid.NewGuid());
        var claims = await Task.WhenAll(a.TryClaimAsync(first, now, now.AddMinutes(1), default).AsTask(),
            b.TryClaimAsync(second, now, now.AddMinutes(1), default).AsTask());
        await Assert.That(claims[0] ^ claims[1]).IsTrue();
        var winner = claims[0] ? first : second;
        var loser = claims[0] ? second : first;
        await Assert.That(await a.GetPendingCountAsync(now, default)).IsEqualTo(0L);
        await Assert.That(await b.RenewClaimAsync(loser, now, now.AddMinutes(2), default)).IsFalse();
        await Assert.That(await b.CompleteAsync(loser, now, default)).IsFalse();
        await Assert.That(await b.RecordFailureAsync(loser, "wrong owner", now, default)).IsFalse();
        await Assert.That(await a.RenewClaimAsync(winner, now, now.AddSeconds(10), default)).IsTrue();
        await Assert.That(await b.TryClaimAsync(loser, now.AddSeconds(11), now.AddMinutes(2), default)).IsFalse();
        await Assert.That(await a.CompleteAsync(winner, now.AddMinutes(1), default)).IsFalse();
        await Assert.That(await a.RenewClaimAsync(winner, now.AddMinutes(1), now.AddMinutes(2), default)).IsFalse();
        await Assert.That(await a.RecordFailureAsync(winner, "expired", now.AddMinutes(1), default)).IsFalse();
        await Assert.That(await a.GetPendingCountAsync(now.AddMinutes(1), default)).IsEqualTo(1L);
        await Assert.That(await b.TryClaimAsync(loser, now.AddMinutes(1), now.AddMinutes(2), default)).IsTrue();
        await Assert.That(await a.CompleteAsync(winner, now, default)).IsFalse();
        await Assert.That(await b.RenewClaimAsync(loser, now.AddMinutes(1), now.AddMinutes(3), default)).IsTrue();
        await Assert.That(await a.TryClaimAsync(new(id, Guid.NewGuid()), now.AddMinutes(2), now.AddMinutes(4), default)).IsFalse();
        await Assert.That(await b.RecordFailureAsync(loser, "broker offline", now.AddMinutes(2), default)).IsTrue();
        await Assert.That(await b.RecordFailureAsync(loser, "duplicate", now.AddMinutes(2), default)).IsFalse();
        var rows = new OutboxRecord[1];
        await Assert.That(await a.ReadPendingAsync(new(null, now.AddMinutes(2)), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Attempts).IsEqualTo(1);
        await Assert.That(rows[0].LastError).IsEqualTo("broker offline");
        var final = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await a.TryClaimAsync(final, now.AddMinutes(2), now.AddMinutes(3), default)).IsTrue();
        await Assert.That(await a.CompleteAsync(final, now.AddMinutes(2), default)).IsTrue();
        await Assert.That(await a.CompleteAsync(final, now.AddMinutes(2), default)).IsFalse();
        await Assert.That(await a.GetPendingCountAsync(now.AddDays(1), default)).IsEqualTo(0L);
    }

    [Test]
    public async Task Claims_obey_the_same_eligibility_rules_and_cancellation_does_not_poison_reuse()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await CreateSchema(source);
        var store = new PostgreSqlOutboxStore(source);
        var now = Microseconds(DateTimeOffset.UtcNow);
        foreach (var kind in new[] { "future", "parked", "live", "missing", "due", "expired" })
        {
            var claim = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
            if (kind != "missing") await Insert(source, new(), claim.MessageId, now,
                deliverAt: kind == "future" ? now.AddHours(1) : now,
                status: kind == "parked" ? "parked" : "pending",
                expires: kind == "live" ? now.AddHours(1) : kind == "expired" ? now : null);
            await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsEqualTo(kind is "due" or "expired");
        }
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await store.GetPendingCountAsync(now, cancelled.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await store.ReadPendingAsync(new(null, now), new OutboxRecord[1], cancelled.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await store.TryClaimAsync(new(Guid.NewGuid(), Guid.NewGuid()), now, now.AddMinutes(1), cancelled.Token));
        await Assert.That(await store.GetPendingCountAsync(now, default)).IsEqualTo(0L);
        await Assert.That(await store.TryClaimAsync(new(Guid.NewGuid(), Guid.NewGuid()), now, now.AddMinutes(1), default)).IsFalse();
    }

    [Test]
    public async Task Blocked_mutations_reject_overlap_and_recover_after_cancellation()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await CreateSchema(source);
        var now = Microseconds(DateTimeOffset.UtcNow);
        var claim = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        await Insert(source, new(), claim.MessageId, now);
        await using var connection = await source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var blocker = new NpgsqlCommand("SELECT id FROM relayer_outbox WHERE id = @id FOR UPDATE", connection, transaction))
        {
            blocker.Parameters.AddWithValue("id", claim.MessageId);
            await blocker.ExecuteScalarAsync();
        }
        var store = new PostgreSqlOutboxStore(source);
        using var cancelled = new CancellationTokenSource();
        var pending = store.TryClaimAsync(claim, now, now.AddMinutes(1), cancelled.Token);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.TryClaimAsync(new(claim.MessageId, Guid.NewGuid()), now, now.AddMinutes(1), default));
        await cancelled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await pending);
        await transaction.RollbackAsync();
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
    }

    [Test]
    public async Task Keyed_scopes_use_immutable_names_and_keep_the_application_data_source_alive()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "custom", TablePrefix = "q_" };
        await CreateSchema(source, options);
        var services = new ServiceCollection();
        services.AddRelayerPostgreSqlOutboxStore("orders", source, options);
        await using (var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true }))
        {
            await using var first = provider.CreateAsyncScope();
            var store = first.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders");
            await Assert.That(store).IsSameReferenceAs(first.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders"));
            await using var second = provider.CreateAsyncScope();
            await Assert.That(store).IsNotSameReferenceAs(second.ServiceProvider.GetRequiredKeyedService<IOutboxStore>("orders"));
            await Assert.That(await store.GetPendingCountAsync(DateTimeOffset.UtcNow, default)).IsEqualTo(0L);
        }
        await using var command = source.CreateCommand("SELECT 1");
        await Assert.That((int)(await command.ExecuteScalarAsync())!).IsEqualTo(1);
    }

    [Test]
    public async Task Invalid_requests_fail_before_database_access_and_database_errors_propagate()
    {
        await using var source = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Timeout=1");
        var store = new PostgreSqlOutboxStore(source);
        var now = DateTimeOffset.UtcNow;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await store.ReadPendingAsync(new(-1, now), new OutboxRecord[1], default));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.CompleteAsync(new(Guid.Empty, Guid.NewGuid()), now, default));
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.CompleteAsync(new(Guid.NewGuid(), Guid.Empty), now, default));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await store.TryClaimAsync(new(Guid.NewGuid(), Guid.NewGuid()), now, now, default));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await store.RenewClaimAsync(new(Guid.NewGuid(), Guid.NewGuid()), now, now.AddTicks(-1), default));
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await store.RecordFailureAsync(new(Guid.NewGuid(), Guid.NewGuid()), null!, now, default));
        await Assert.ThrowsAsync<NpgsqlException>(async () => await store.GetPendingCountAsync(now, default));
        await Assert.ThrowsAsync<NpgsqlException>(async () => await store.GetPendingCountAsync(now, default));
    }

    [Test]
    public async Task Batch_claims_match_input_positions_and_exclude_ineligible_or_competing_rows()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "Claim\"Schema", TablePrefix = "test_" };
        await CreateSchema(source, options);
        var store = new PostgreSqlOutboxStore(source, options);
        var now = Microseconds(DateTimeOffset.UtcNow);
        var kinds = new[] { "due", "future", "live", "expired", "parked", "missing", "ordered", "due" };
        var claims = new OutboxClaim[kinds.Length];
        var claimed = Enumerable.Repeat(true, kinds.Length).ToArray();
        for (var i = 0; i < kinds.Length; i++)
        {
            claims[i] = new(Guid.CreateVersion7(), Guid.NewGuid());
            if (kinds[i] == "missing") continue;
            await Insert(source, options, claims[i].MessageId, now,
                lane: kinds[i] == "ordered" ? 1 : null,
                deliverAt: kinds[i] == "future" ? now.AddHours(1) : now,
                status: kinds[i] == "parked" ? "parked" : "pending",
                expires: kinds[i] == "live" ? now.AddHours(1) : kinds[i] == "expired" ? now : null);
        }
        await store.ClaimBatchAsync(claims, claimed, now, now.AddMinutes(1), default);
        var expected = new[] { true, false, false, true, false, false, false, true };
        for (var i = 0; i < claims.Length; i++)
        {
            await Assert.That(claimed[i]).IsEqualTo(expected[i]);
            if (!claimed[i]) continue;
            await Assert.That(await store.CompleteAsync(new(claims[i].MessageId, Guid.NewGuid()), now, default)).IsFalse();
            await Assert.That(await store.RenewClaimAsync(claims[i], now, now.AddMinutes(1), default)).IsTrue();
        }
        var competitor = new PostgreSqlOutboxStore(source, options);
        var competingClaims = claims.Select(claim => new OutboxClaim(claim.MessageId, Guid.NewGuid())).ToArray();
        var competingResults = new bool[claims.Length];
        await competitor.ClaimBatchAsync(competingClaims, competingResults, now, now.AddMinutes(2), default);
        await Assert.That(competingResults.Any(result => result)).IsFalse();
        await competitor.ClaimBatchAsync(competingClaims, competingResults, now.AddMinutes(1), now.AddMinutes(2), default);
        for (var i = 0; i < claims.Length; i++)
            await Assert.That(competingResults[i]).IsEqualTo(expected[i]);
        await Assert.That(await store.CompleteBatchAsync(claims, now.AddMinutes(1), default)).IsEqualTo(0L);
        await Assert.That(await competitor.CompleteBatchAsync(competingClaims, now.AddMinutes(1), default)).IsEqualTo(3L);
        await store.ClaimBatchAsync(claims, claimed, now, now.AddMinutes(1), default);
        await Assert.That(claimed.Any(result => result)).IsFalse();
        await store.ClaimBatchAsync(ReadOnlyMemory<OutboxClaim>.Empty, Memory<bool>.Empty, now, now.AddMinutes(1), default);
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await store.ClaimBatchAsync(claims, Memory<bool>.Empty, now, now.AddMinutes(1), default));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await store.ClaimBatchAsync(claims, claimed, now, now, default));
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await store.ClaimBatchAsync(claims, claimed, now, now.AddMinutes(1), new CancellationToken(true)));
    }

    [Test]
    public async Task Batch_completion_fences_each_row_and_deletes_only_live_matching_claims_in_one_statement()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        var options = new PostgreSqlOutboxOptions { Schema = "Batch\"Schema", TablePrefix = "test_" };
        await CreateSchema(source, options);
        var table = "\"Batch\"\"Schema\".\"test_outbox\"";
        var store = new PostgreSqlOutboxStore(source, options);
        var now = Microseconds(DateTimeOffset.UtcNow);
        var first = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        var second = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        var expired = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        var replaced = new OutboxClaim(Guid.CreateVersion7(), Guid.NewGuid());
        await Insert(source, options, first.MessageId, now);
        await Insert(source, options, second.MessageId, now);
        await Insert(source, options, expired.MessageId, now);
        await Insert(source, options, replaced.MessageId, now);
        await store.TryClaimAsync(first, now, now.AddMinutes(1), default);
        await store.TryClaimAsync(second, now, now.AddMinutes(1), default);
        await store.TryClaimAsync(expired, now, now.AddSeconds(1), default);
        await store.TryClaimAsync(new(replaced.MessageId, Guid.NewGuid()), now, now.AddMinutes(1), default);
        await using (var command = source.CreateCommand($"""
            CREATE TABLE delete_log (id integer);
            CREATE FUNCTION log_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO delete_log VALUES (1); RETURN NULL; END; $$;
            CREATE TRIGGER delete_count AFTER DELETE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION log_delete();
            """)) await command.ExecuteNonQueryAsync();
        OutboxClaim[] claims = [first, second, expired, replaced, new(Guid.CreateVersion7(), Guid.NewGuid()), first];
        await Assert.That(await store.CompleteBatchAsync(claims, now.AddSeconds(1), default)).IsEqualTo(2L);
        await using (var command = source.CreateCommand($"SELECT count(*) FROM {table}"))
            await Assert.That((long)(await command.ExecuteScalarAsync())!).IsEqualTo(2L);
        await using (var command = source.CreateCommand("SELECT count(*) FROM delete_log"))
            await Assert.That((long)(await command.ExecuteScalarAsync())!).IsEqualTo(1L);
        await Assert.That(await store.CompleteBatchAsync(claims, now.AddSeconds(1), default)).IsEqualTo(0L);
        await Assert.That(await store.CompleteBatchAsync(ReadOnlyMemory<OutboxClaim>.Empty, now, default)).IsEqualTo(0L);
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await store.CompleteBatchAsync(new OutboxClaim[] { new(Guid.Empty, Guid.NewGuid()) }, now, default));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await store.CompleteBatchAsync(claims, now, cancellation.Token));
    }

    internal static async Task CreateSchema(NpgsqlDataSource source, PostgreSqlOutboxOptions? options = null)
    {
        await using var command = source.CreateCommand(PostgreSqlOutboxSchema.CreateScript(options));
        await command.ExecuteNonQueryAsync();
    }

    internal static async Task Insert(NpgsqlDataSource source, PostgreSqlOutboxOptions options, Guid id, DateTimeOffset now,
        int? lane = null, DateTimeOffset? deliverAt = null, string status = "pending", DateTimeOffset? expires = null)
    {
        var buffer = new ArrayBufferWriter<byte>();
        OutboxHeaderCodec.Write(buffer, [new("duplicate", new("é")), new("duplicate", new(new byte[] { 255, 0, 254 })), new("empty", new(""))]);
        var table = PostgreSqlOutboxSchema.QuoteIdentifier(options.Schema ?? "public") + "." + PostgreSqlOutboxSchema.QuoteIdentifier(options.TablePrefix + "outbox");
        await using var command = source.CreateCommand($"""
            INSERT INTO {table} (id, lane, transport, destination, message_type, headers, body, ordering_key, created_at, deliver_at, status, claim_token, claim_expires_at, tenant_id)
            VALUES (@id, @lane, 'events', 'orders', 'order.placed.v1', @headers, @body, 'customer-é', @now, @due, @status, @token, @expires, 'tenant')
            """);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("lane", NpgsqlDbType.Integer, (object?)lane ?? DBNull.Value);
        command.Parameters.AddWithValue("headers", buffer.WrittenMemory.ToArray());
        command.Parameters.AddWithValue("body", new byte[] { 0, 255, 1 });
        command.Parameters.AddWithValue("now", now.ToUniversalTime());
        command.Parameters.AddWithValue("due", NpgsqlDbType.TimestampTz, (object?)deliverAt?.ToUniversalTime() ?? DBNull.Value);
        command.Parameters.AddWithValue("status", status);
        command.Parameters.AddWithValue("token", NpgsqlDbType.Uuid, expires is null ? DBNull.Value : Guid.NewGuid());
        command.Parameters.AddWithValue("expires", NpgsqlDbType.TimestampTz, (object?)expires?.ToUniversalTime() ?? DBNull.Value);
        await command.ExecuteNonQueryAsync();
    }

    private static DateTimeOffset Microseconds(DateTimeOffset value) => new(value.Ticks - value.Ticks % 10, value.Offset);
}
