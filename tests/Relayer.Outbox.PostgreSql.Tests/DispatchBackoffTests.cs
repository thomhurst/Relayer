using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

public class DispatchBackoffTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Outage_exceeding_poison_budget_persists_backoff_and_recovers_after_restart(bool ordered)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { Lanes = 1, MaxDispatchAttempts = 1 };
        var id = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow, lane: ordered ? 0 : null);
        if (!ordered)
        {
            await using var unordered = source.CreateCommand("UPDATE relayer_outbox SET ordering_key = NULL");
            await unordered.ExecuteNonQueryAsync();
        }
        var transport = new OutageTransport();
        var rows = new OutboxRecord[1];
        for (var attempt = 0; attempt < 12; attempt++)
        {
            // Advance database eligibility rather than sleeping. Each attempt uses a fresh worker,
            // proving that retries and ordered head barriers survive worker/process replacement.
            await using (var ready = source.CreateCommand("UPDATE relayer_outbox SET deliver_at = clock_timestamp(); UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second'"))
                await ready.ExecuteNonQueryAsync();
            await using var clock = source.CreateCommand("SELECT clock_timestamp()");
            var before = (DateTime)(await clock.ExecuteScalarAsync())!;
            await Dispatch();
            await using var state = source.CreateCommand("SELECT attempts, status, deliver_at, clock_timestamp(), claim_token IS NULL, (SELECT count(*) FROM relayer_outbox_dead) FROM relayer_outbox");
            await using var reader = await state.ExecuteReaderAsync();
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(reader.GetInt32(0)).IsEqualTo(attempt + 1);
            await Assert.That(reader.GetString(1)).IsEqualTo("pending");
            var due = reader.GetDateTime(2);
            var after = reader.GetDateTime(3);
            var expected = Math.Min(300, Math.Pow(2, Math.Min(attempt, 9)));
            await Assert.That(due >= before.AddSeconds(expected) && due <= after.AddSeconds(expected)).IsTrue();
            await Assert.That(reader.GetBoolean(4)).IsTrue();
            await Assert.That(reader.GetInt64(5)).IsEqualTo(0L);
            await reader.DisposeAsync();
            // Test due-time fencing independently of test-runner scheduling pauses.
            await using (var future = source.CreateCommand("UPDATE relayer_outbox SET deliver_at = clock_timestamp() + interval '1 day'"))
                await future.ExecuteNonQueryAsync();
            await Dispatch();
            await Assert.That(transport.Attempts).IsEqualTo(attempt + 1);
        }
        transport.Failing = false;
        await using (var ready = source.CreateCommand("UPDATE relayer_outbox SET deliver_at = clock_timestamp(); UPDATE relayer_leases SET expires_at = clock_timestamp() - interval '1 second'"))
            await ready.ExecuteNonQueryAsync();
        await Dispatch();
        await using var remaining = source.CreateCommand("SELECT count(*) FROM relayer_outbox");
        await Assert.That((long)(await remaining.ExecuteScalarAsync())!).IsEqualTo(0L);
        await Assert.That(transport.Attempts).IsEqualTo(13);

        async Task Dispatch()
        {
            if (ordered)
            {
                using var worker = new PostgreSqlOrderedDispatcher(source, new() { Lanes = 1, MaxDispatchAttempts = 1 }, [transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance);
                await worker.DispatchLaneAsync(0, default);
            }
            else
            {
                var store = new PostgreSqlOutboxStore(source, options);
                var count = await store.ReadPendingAsync(new(null, DateTimeOffset.UtcNow), rows, default);
                using var worker = new PostgreSqlDirectDispatcher(source, options, new(), [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
                await worker.DispatchRecoveredAsync(rows.AsMemory(0, count), default);
            }
        }
    }

    [Test]
    public async Task Retry_is_fenced_rolls_back_and_saturates_without_poisoning()
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = NpgsqlDataSource.Create(fixture.ConnectionString);
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        var options = new PostgreSqlOutboxOptions { MaxDispatchAttempts = 1 };
        var id = Guid.CreateVersion7();
        await PostgreSqlOutboxStoreTests.Insert(source, options, id, DateTimeOffset.UtcNow);
        await using (var seed = source.CreateCommand("UPDATE relayer_outbox SET attempts = 30"))
            await seed.ExecuteNonQueryAsync();
        var store = new PostgreSqlOutboxStore(source, options);
        var claim = new OutboxClaim(id, Guid.NewGuid());
        var now = DateTimeOffset.UtcNow;
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordDispatchFailureAsync(new(id, Guid.NewGuid()), "stale", now, default, retryable: true)).IsEqualTo(0);
        await Assert.That(await store.RecordDispatchFailureAsync(claim, "expired", now.AddMinutes(2), default, retryable: true)).IsEqualTo(0);
        await using (var connection = await source.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            store.UseDispatchTransaction(connection, transaction);
            await Assert.That(await store.RecordDispatchFailureAsync(claim, "rollback", now, default, retryable: true)).IsEqualTo(1);
            await transaction.RollbackAsync();
            store.UseDispatchTransaction(null, null);
        }
        await Assert.That(await store.RecordDispatchFailureAsync(claim, "retry", now, default, retryable: true)).IsEqualTo(1);
        await Assert.That(await store.RecordDispatchFailureAsync(claim, "replay", now, default, retryable: true)).IsEqualTo(0);
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(null, now.AddSeconds(299)), rows, default)).IsEqualTo(0);
        await Assert.That(await store.ReadPendingAsync(new(null, now.AddSeconds(301)), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Attempts).IsEqualTo(30);
        await Assert.That(rows[0].LastError).IsEqualTo("retry");
    }

    private sealed class OutageTransport : ITransport
    {
        public bool Failing { get; set; } = true;
        public int Attempts { get; private set; }
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            Attempts++;
            if (Failing) throw new IOException("broker unavailable");
            return ValueTask.CompletedTask;
        }
        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken) => SendAsync(messages.Span[0], cancellationToken);
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
