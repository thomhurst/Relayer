using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class DispatchBackoffTests(SqlServerFixture database)
{
    [Test]
    [Arguments("direct")]
    [Arguments("maintenance")]
    [Arguments("ordered")]
    public async Task Native_outage_beyond_poison_budget_retains_deadlines_and_recovers_after_worker_replacement(string mode)
    {
        var data = new StoreData(database, new()
        {
            TablePrefix = "backoff_" + Guid.NewGuid().ToString("N") + "_", Lanes = 1, MaxDispatchAttempts = 2
        });
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow.AddDays(-1), mode == "ordered" ? 0 : null, orderedHeaders: true);
        var transport = new Transport();
        using var ordered = Ordered(data, transport);
        int[] delays = [1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300];
        for (var index = 0; index < delays.Length; index++)
        {
            // Unordered fenced mutations use the caller's clock; ordered transactions use SQL's clock.
            var before = mode == "ordered" ? await DatabaseTime() : DateTimeOffset.UtcNow;
            await Dispatch(mode, data, transport, ordered);
            var after = mode == "ordered" ? await DatabaseTime() : DateTimeOffset.UtcNow;
            var row = await Read(data);
            await Assert.That(row.Attempts).IsEqualTo(index + 1);
            await Assert.That(row.Error).IsEqualTo("broker offline");
            await Assert.That(row.Status).IsEqualTo("pending");
            await Assert.That(row.Claimed).IsFalse();
            await Assert.That(row.Due >= before.AddSeconds(delays[index])).IsTrue();
            await Assert.That(row.Due <= after.AddSeconds(delays[index])).IsTrue();
            await Assert.That(await Count(SqlServerOutboxSchema.DeadTable(data.Options))).IsEqualTo(0L);

            // A fresh direct/maintenance worker or another lane pass respects the persisted deadline.
            // Large deadlines keep this deterministic even on a slow database.
            if (index == delays.Length - 1)
            {
                var calls = transport.Calls;
                await Dispatch(mode, data, transport, ordered);
                await Assert.That(transport.Calls).IsEqualTo(calls);
                await Assert.That(await Read(data)).IsEqualTo(row);
            }
            await data.Execute($"UPDATE {data.Table} SET deliver_at = DATEADD(second, -1, SYSUTCDATETIME())");
        }
        transport.Offline = false;
        // Replacement loses every in-memory buffer and counter. Only the persisted row survives.
        if (mode == "ordered")
            await data.Execute($"UPDATE {SqlServerOutboxSchema.OrderedLeaseTable(data.Options)} SET expires_at = DATEADD(second, -1, SYSUTCDATETIME())");
        using var replacement = Ordered(data, transport);
        await Dispatch(mode, data, transport, replacement);
        await Assert.That(await Count(data.Table)).IsEqualTo(0L);
        await Assert.That(await Count(SqlServerOutboxSchema.DeadTable(data.Options))).IsEqualTo(0L);
        await Assert.That(transport.Sent).IsEqualTo(1);
        await Assert.That(transport.LastDeliverAt).IsNull();
        await Assert.That(transport.LastBody).IsEquivalentTo(new byte[] { 0, 255, 1 });
    }

    [Test]
    public async Task Retry_mutation_fences_expired_and_replaced_tokens_and_saturates_without_poison()
    {
        var data = new StoreData(database, new()
        { TablePrefix = "fence_" + Guid.NewGuid().ToString("N") + "_", MaxDispatchAttempts = 1 });
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow);
        var store = data.Store();
        var now = DateTimeOffset.UtcNow;
        var first = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(first, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordRetryAsync(first, "offline", now.AddMinutes(2), default)).IsFalse();
        var replacement = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(replacement, now.AddMinutes(2), now.AddMinutes(3), default)).IsTrue();
        await Assert.That(await store.RecordRetryAsync(first, "stale", now.AddMinutes(2), default)).IsFalse();
        await data.Execute($"UPDATE {data.Table} SET attempts = 2147483647");
        await Assert.That(await store.RecordRetryAsync(replacement, "offline", now.AddMinutes(2), default)).IsTrue();
        var row = await Read(data);
        await Assert.That(row.Attempts).IsEqualTo(30);
        await Assert.That(row.Due).IsEqualTo(now.AddMinutes(7));
        await Assert.That(row.Status).IsEqualTo("pending");
        await Assert.That(row.Claimed).IsFalse();
        await Assert.That(await store.RecordRetryAsync(replacement, "duplicate", now.AddMinutes(2), default)).IsFalse();
        await Assert.That(await Read(data)).IsEqualTo(row);
        await Assert.That(await store.TryClaimAsync(new(id, Guid.NewGuid()), now.AddMinutes(6), now.AddMinutes(8), default)).IsFalse();
        await Assert.That(await Count(SqlServerOutboxSchema.DeadTable(data.Options))).IsEqualTo(0L);
        // Explicit low-level permanent failures still take the original poison transition.
        var permanent = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(permanent, now.AddMinutes(8), now.AddMinutes(9), default)).IsTrue();
        await Assert.That(await store.RecordFailureAsync(permanent, "permanent", now.AddMinutes(8), default)).IsTrue();
        await Assert.That(await Count(data.Table)).IsEqualTo(0L);
        await Assert.That(await Count(SqlServerOutboxSchema.DeadTable(data.Options))).IsEqualTo(1L);
    }

    [Test]
    public async Task Ordered_retry_rollback_and_stale_claims_cannot_change_a_new_owner()
    {
        var data = new StoreData(database);
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, DateTimeOffset.UtcNow, 0, orderedHeaders: true);
        using var lane = new SqlServerOrderedLane(ConnectionString, data.Options, TimeSpan.FromSeconds(30), 1);
        var stale = new OutboxClaim(id, Guid.NewGuid());
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
        {
            await Assert.That(await lane.ClaimAsync(connection, transaction, stale, default)).IsTrue();
            await Assert.That(await lane.RetryAsync(connection, transaction, stale, "rolled back", default)).IsTrue();
            await transaction.RollbackAsync();
        }
        await Assert.That((await Read(data)).Attempts).IsEqualTo(0);
        await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
        {
            var live = new OutboxClaim(id, Guid.NewGuid());
            await Assert.That(await lane.ClaimAsync(connection, transaction, live, default)).IsTrue();
            await Assert.That(await lane.RetryAsync(connection, transaction, stale, "stale", default)).IsFalse();
            await using var expire = new SqlCommand($"UPDATE {data.Table} SET claim_expires_at = DATEADD(second,-1,SYSUTCDATETIME())", connection, transaction);
            await expire.ExecuteNonQueryAsync();
            await Assert.That(await lane.RetryAsync(connection, transaction, live, "expired", default)).IsFalse();
            await transaction.CommitAsync();
        }
        await Assert.That((await Read(data)).Attempts).IsEqualTo(0);
        var transport = new Transport { Offline = false };
        using var worker = Ordered(data, transport);
        await Assert.That(await worker.DispatchLaneAsync(0, default)).IsEqualTo(1);
        await Assert.That(await Count(data.Table)).IsEqualTo(0L);
    }

    private string ConnectionString => new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" }.ConnectionString;

    private SqlServerOrderedDispatcher Ordered(StoreData data, Transport transport) => new(ConnectionString,
        new() { TablePrefix = data.Options.TablePrefix, Lanes = 1, MaxDispatchAttempts = 2 },
        [transport], NullLogger<SqlServerOrderedDispatcher>.Instance);

    private async Task Dispatch(string mode, StoreData data, Transport transport, SqlServerOrderedDispatcher ordered)
    {
        if (mode == "ordered") { await ordered.DispatchLaneAsync(0, default); return; }
        if (mode == "maintenance")
        {
            using var worker = new SqlServerMaintenanceWorker(ConnectionString,
                new() { TablePrefix = data.Options.TablePrefix, RecoveryDelay = TimeSpan.Zero, MaxDispatchAttempts = 2 },
                [transport], NullLogger<SqlServerMaintenanceWorker>.Instance);
            try { await worker.SweepAsync(); }
            finally { await worker.ReleaseAsync(); }
            return;
        }
        // Use the actual commit-notification queue and drain it before inspecting durable state.
        using var direct = new SqlServerDirectDispatcher(ConnectionString, data.Options, new(), [transport],
            NullLogger<SqlServerDirectDispatcher>.Instance);
        var records = new OutboxRecord[1];
        if (await data.Store().ReadPendingAsync(new(null, DateTimeOffset.MaxValue), records, default) == 1)
        {
            // Simulate replay of an old commit notification: its buffer has no retry deadline.
            // Only the database claim predicate can stop this stale buffer from sending too early.
            var record = records[0];
            record = record with { Message = new(record.Message.Destination, record.Message.Body, record.Message.Headers) };
            var buffer = new SqlServerBufferedMessage();
            buffer.Copy(record, 1, unordered: true);
            direct.Committed([buffer], 1);
        }
        await direct.StartAsync(default);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await direct.StopAsync(timeout.Token);
    }

    private sealed record Row(int Attempts, string? Error, string Status, DateTimeOffset? Due, bool Claimed);

    private async Task<Row> Read(StoreData data)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT attempts, last_error, status, deliver_at, claim_token FROM {data.Table}", connection);
        await using var reader = await command.ExecuteReaderAsync();
        await Assert.That(await reader.ReadAsync()).IsTrue();
        return new(reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetDateTimeOffset(3), !reader.IsDBNull(4));
    }

    private async Task<DateTimeOffset> DatabaseTime()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand("SELECT CONVERT(datetimeoffset, SYSUTCDATETIME())", connection);
        return (DateTimeOffset)(await command.ExecuteScalarAsync())!;
    }

    private async Task<long> Count(string table)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT COUNT_BIG(*) FROM {table}", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private sealed class Transport : ITransport
    {
        internal bool Offline { get; set; } = true;
        internal int Calls { get; private set; }
        internal int Sent { get; private set; }
        internal DateTimeOffset? LastDeliverAt { get; private set; }
        internal byte[]? LastBody { get; private set; }
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true, MaxBatchSize = 10 };
        public ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            Calls++;
            if (Offline) return ValueTask.FromException(new IOException("broker offline"));
            Sent++;
            LastDeliverAt = message.DeliverAt;
            LastBody = message.Body.ToArray();
            return ValueTask.CompletedTask;
        }
        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
        {
            for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], token);
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => new(new TopologyReport([]));
        public Task StartAsync(CancellationToken token) => Task.CompletedTask;
        public Task StopAsync(CancellationToken token) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
