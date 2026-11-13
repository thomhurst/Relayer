using System.Data;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using Relayer.Transports;

namespace Relayer.Outbox.MySql.Tests;

public sealed class DispatchBackoffTests
{
    [Test]
    [Arguments("direct")]
    [Arguments("maintenance")]
    [Arguments("ordered")]
    public async Task Outage_beyond_poison_budget_persists_bounded_backoff_and_recovers_after_restart(string mode)
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        var ordered = mode == "ordered";
        var id = Guid.CreateVersion7();
        await database.Insert(id, DateTimeOffset.UtcNow.AddDays(-1), lane: ordered ? 0 : null);
        if (!ordered) await database.Execute("UPDATE relayer_outbox SET ordering_key=NULL");
        var options = new MySqlOutboxOptions { Lanes = 1, MaxDispatchAttempts = 1 };
        var transport = new OutageTransport();
        using var instrumentation = new RelayerInstrumentation();
        long pending = 0, poison = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument == instrumentation.OutboxPending || instrument == instrumentation.OutboxPoison)
                meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, count, _, _) =>
        {
            if (instrument == instrumentation.OutboxPending) Interlocked.Add(ref pending, count);
            else Interlocked.Add(ref poison, count);
        });
        listener.Start();
        instrumentation.AddOutboxPending(1);
        var rows = new OutboxRecord[1];
        using (var store = new MySqlOutboxStore(database.Source, options))
            await store.ReadPendingAsync(new(ordered ? 0 : null, DateTimeOffset.UtcNow), rows, default);
        for (var attempt = 0; attempt < 12; attempt++)
        {
            await MakeReady();
            var before = await Clock();
            await Dispatch();
            await using var state = database.Source.CreateCommand("SELECT attempts, status, deliver_at, UTC_TIMESTAMP(6), claim_token IS NULL AND claim_expires_at IS NULL, (SELECT COUNT(*) FROM relayer_dead) FROM relayer_outbox");
            await using var reader = await state.ExecuteReaderAsync();
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(reader.GetInt32(0)).IsEqualTo(attempt + 1);
            await Assert.That(reader.GetString(1)).IsEqualTo("pending");
            var due = reader.GetDateTime(2);
            var after = reader.GetDateTime(3);
            var delay = Math.Min(300, Math.Pow(2, Math.Min(attempt, 9)));
            await Assert.That(due >= before.AddSeconds(delay) && due <= after.AddSeconds(delay)).IsTrue();
            await Assert.That(reader.GetBoolean(4)).IsTrue();
            await Assert.That(reader.GetInt64(5)).IsEqualTo(0L);
            await Assert.That(poison).IsEqualTo(0L);
            await Assert.That(pending).IsEqualTo(1L);
            await reader.DisposeAsync();
            // Independent of scheduler pauses: a stale notification and a new worker must obey eligibility.
            await database.Execute("UPDATE relayer_outbox SET deliver_at=TIMESTAMPADD(DAY,1,UTC_TIMESTAMP(6)); UPDATE relayer_leases SET expires_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6))");
            await Dispatch();
            await Assert.That(transport.Attempts).IsEqualTo(attempt + 1);
        }
        transport.Failing = false;
        await MakeReady();
        await Dispatch();
        await Assert.That(await Scalar(database, "SELECT COUNT(*) FROM relayer_outbox")).IsEqualTo(0L);
        await Assert.That(transport.Attempts).IsEqualTo(13);
        await Assert.That(poison).IsEqualTo(0L);
        await Assert.That(pending).IsEqualTo(0L);

        Task MakeReady() => database.Execute("UPDATE relayer_outbox SET deliver_at=UTC_TIMESTAMP(6); UPDATE relayer_leases SET expires_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6))");
        async Task<DateTime> Clock()
        {
            await using var command = database.Source.CreateCommand("SELECT UTC_TIMESTAMP(6)");
            return (DateTime)(await command.ExecuteScalarAsync())!;
        }
        async Task Dispatch()
        {
            // Fresh instances prove that backoff does not depend on process memory.
            if (ordered)
            {
                using var worker = new MySqlOrderedDispatcher(database.Source, new() { Lanes = 1, MaxDispatchAttempts = 1 }, [transport], NullLogger<MySqlOrderedDispatcher>.Instance, instrumentation: instrumentation);
                await worker.DispatchLaneAsync(0, default);
            }
            else if (mode == "maintenance")
            {
                using var worker = new MySqlMaintenanceWorker(database.Source, new() { MaxDispatchAttempts = 1, RecoveryDelay = TimeSpan.Zero }, [transport], NullLogger<MySqlMaintenanceWorker>.Instance, instrumentation: instrumentation);
                await worker.SweepAsync();
            }
            else
            {
                using var worker = new MySqlDirectDispatcher(database.Source, options, new(), [transport], NullLogger<MySqlDirectDispatcher>.Instance, instrumentation: instrumentation);
                var buffered = new BufferedOutboxMessage();
                buffered.Copy(rows[0], 1);
                worker.Committed([buffered], 1);
                await worker.StartAsync(default);
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await worker.StopAsync(timeout.Token);
            }
        }
    }

    [Test]
    public async Task Unordered_retry_is_fenced_atomic_and_saturates_without_dead_lettering()
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        var id = Guid.CreateVersion7();
        await database.Insert(id, DateTimeOffset.UtcNow);
        await database.Execute("UPDATE relayer_outbox SET attempts=2147483647");
        using var store = new MySqlOutboxStore(database.Source, new() { MaxDispatchAttempts = 1 });
        var now = DateTimeOffset.UtcNow;
        var claim = new OutboxClaim(id, Guid.NewGuid());
        await Assert.That(await store.TryClaimAsync(claim, now, now.AddMinutes(1), default)).IsTrue();
        await Assert.That(await store.RecordRetryAsync(new(id, Guid.NewGuid()), "stale", now, default)).IsFalse();
        await Assert.That(await store.RecordRetryAsync(claim, "expired", now.AddMinutes(2), default)).IsFalse();
        await database.Execute("CREATE TRIGGER reject_retry BEFORE UPDATE ON relayer_outbox FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='write unavailable'");
        await Assert.That(async () => await store.RecordRetryAsync(claim, "rollback", now, default)).Throws<MySqlException>();
        await Assert.That(await Scalar(database, "SELECT COUNT(*) FROM relayer_outbox WHERE attempts=2147483647 AND claim_token IS NOT NULL AND deliver_at IS NULL AND last_error IS NULL")).IsEqualTo(1L);
        await database.Execute("DROP TRIGGER reject_retry");
        await Assert.That(await store.RecordRetryAsync(claim, "retry", now, default)).IsTrue();
        await Assert.That(await store.RecordRetryAsync(claim, "replay", now, default)).IsFalse();
        var rows = new OutboxRecord[1];
        await Assert.That(await store.ReadPendingAsync(new(null, now.AddSeconds(299)), rows, default)).IsEqualTo(0);
        await Assert.That(await store.ReadPendingAsync(new(null, now.AddSeconds(301)), rows, default)).IsEqualTo(1);
        await Assert.That(rows[0].Attempts).IsEqualTo(30);
        await Assert.That(rows[0].LastError).IsEqualTo("retry");
        await Assert.That(await Scalar(database, "SELECT COUNT(*) FROM relayer_dead")).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Ordered_retry_rejects_stale_claims_and_rolls_back_with_lane_transaction(bool replace)
    {
        await using var database = new MySqlFixture();
        await database.StartAsync();
        var id = Guid.CreateVersion7();
        await database.Insert(id, DateTimeOffset.UtcNow, lane: 0);
        using var lane = new MySqlOrderedLane(database.Source, new(), TimeSpan.FromMinutes(1), 1);
        await using var connection = await database.Source.OpenConnectionAsync();
        await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted))
        {
            var claim = new OutboxClaim(id, Guid.NewGuid());
            await Assert.That(await lane.ClaimAsync(connection, transaction, claim, default)).IsTrue();
            await using var change = connection.CreateCommand();
            change.Transaction = transaction;
            change.CommandText = replace ? "UPDATE relayer_outbox SET claim_token=UUID_TO_BIN(UUID())" : "UPDATE relayer_outbox SET claim_expires_at=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6))";
            await change.ExecuteNonQueryAsync();
            await Assert.That(await lane.RetryAsync(connection, transaction, claim, "stale", default)).IsFalse();
            await transaction.RollbackAsync();
        }
        await using (var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted))
        {
            var claim = new OutboxClaim(id, Guid.NewGuid());
            await Assert.That(await lane.ClaimAsync(connection, transaction, claim, default)).IsTrue();
            await Assert.That(await lane.RetryAsync(connection, transaction, claim, "rollback", default)).IsTrue();
            await Assert.That(await lane.RetryAsync(connection, transaction, claim, "replay", default)).IsFalse();
            await transaction.RollbackAsync();
        }
        await Assert.That(await Scalar(database, "SELECT COUNT(*) FROM relayer_outbox WHERE attempts=0 AND claim_token IS NULL AND deliver_at IS NULL AND last_error IS NULL AND status='pending'")).IsEqualTo(1L);
    }

    private static async Task<long> Scalar(MySqlFixture database, string sql)
    {
        await using var command = database.Source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private sealed class OutageTransport : ITransport
    {
        internal bool Failing { get; set; } = true;
        internal int Attempts { get; private set; }
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
