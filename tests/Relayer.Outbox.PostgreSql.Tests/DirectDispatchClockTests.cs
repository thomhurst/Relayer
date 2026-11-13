using Microsoft.Extensions.Logging.Abstractions;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;
using Transport = Relayer.TestFixtures.OutboxDirectDispatchContractTests.RecordingTransport;

namespace Relayer.Outbox.PostgreSql.Tests;

public class DirectDispatchClockTests
{
    [Test]
    [Arguments(-24, false)]
    [Arguments(24, false)]
    [Arguments(-24, true)]
    [Arguments(24, true)]
    public async Task Database_clock_controls_claim_eligibility_and_completion_with_offset_host_clock(int hours, bool recovery)
    {
        await using var database = new DirectDispatchTests.Database();
        await database.Start();
        var transport = new Transport { Block = true };
        await using var provider = database.Services(transport, enableDispatch: false);
        var now = await DatabaseTime(database);
        // A clock behind the database must not filter a due scheduled row during handoff.
        var due = Record(now.AddMinutes(-1));
        var future = Record(now.AddHours(1));
        await database.Commit(provider, due);
        await database.Commit(provider, future);
        await using (var expired = database.Source.CreateCommand($"UPDATE {DirectDispatchTests.Database.Table} SET claim_token = gen_random_uuid(), claim_expires_at = clock_timestamp() - interval '1 second' WHERE id = @id"))
        {
            expired.Parameters.AddWithValue("id", due.Id);
            await expired.ExecuteNonQueryAsync();
        }
        var clock = new Clock(now.AddHours(hours));
        using var dispatcher = Dispatcher(database, transport, clock, retainSent: true);
        var dispatch = Dispatch(dispatcher, [due, future], recovery);
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        try
        {
            await Assert.That(await database.Scalar($"SELECT count(*) FROM {DirectDispatchTests.Database.Table} WHERE claim_expires_at > clock_timestamp() AND claim_expires_at < clock_timestamp() + interval '2 minutes'"))
                .IsEqualTo(1L);
            var competitor = new PostgreSqlOutboxStore(database.Source,
                new() { Schema = DirectDispatchTests.Database.Schema, TablePrefix = "test_" });
            var claimed = new bool[1];
            await competitor.ClaimDirectBatchAsync(new[] { new OutboxClaim(due.Id, Guid.NewGuid()) }, claimed, TimeSpan.FromMinutes(1), default);
            await Assert.That(claimed[0]).IsFalse();
            // A host clock jump during a send must not change the fenced outcome.
            clock.UtcNow = now.AddHours(-hours);
        }
        finally { transport.Release.TrySetResult(); }
        await dispatch;
        await Assert.That(transport.Received.Count).IsEqualTo(1);
        await using (var sent = database.Source.CreateCommand("SELECT count(*) FROM \"Dispatch\"\"Schema\".test_outbox_sent WHERE sent_at >= @before AND sent_at <= clock_timestamp()"))
        {
            sent.Parameters.AddWithValue("before", now);
            await Assert.That((long)(await sent.ExecuteScalarAsync())!).IsEqualTo(1L);
        }
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar($"SELECT count(*) FROM {DirectDispatchTests.Database.Table} WHERE claim_token IS NULL"))
            .IsEqualTo(1L);
    }

    [Test]
    public async Task Monotonic_claim_budget_skips_send_after_slow_claim_response()
    {
        await using var database = new DirectDispatchTests.Database();
        await database.Start();
        var transport = new Transport();
        await using var provider = database.Services(transport, enableDispatch: false);
        var row = Record(null);
        await database.Commit(provider, row);
        // First timestamp starts the claim; the next read models a delayed response without sleeping.
        var clock = new Clock(DateTimeOffset.UtcNow) { AdvanceOnRead = TimeSpan.FromMinutes(2).Ticks };
        using var dispatcher = Dispatcher(database, transport, clock);
        await Dispatch(dispatcher, [row], false);
        await Assert.That(transport.Calls.Count).IsEqualTo(0);
        await Assert.That(await database.Count()).IsEqualTo(1L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Completion_rejects_expired_or_replaced_claim_even_when_host_is_behind(bool replace)
    {
        await using var database = new DirectDispatchTests.Database();
        await database.Start();
        var transport = new Transport { Block = true };
        await using var provider = database.Services(transport, enableDispatch: false);
        var row = Record(null);
        await database.Commit(provider, row);
        using var dispatcher = Dispatcher(database, transport, new Clock(DateTimeOffset.UtcNow.AddDays(-1)));
        var dispatch = Dispatch(dispatcher, [row], false);
        await transport.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        try
        {
            var mutation = replace ? "claim_token = gen_random_uuid()" : "claim_expires_at = clock_timestamp() - interval '1 second'";
            await using var command = database.Source.CreateCommand($"UPDATE {DirectDispatchTests.Database.Table} SET {mutation}");
            await command.ExecuteNonQueryAsync();
        }
        finally { transport.Release.TrySetResult(); }
        await dispatch;
        await Assert.That(await database.Count()).IsEqualTo(1L);
        await Assert.That(await database.Scalar($"SELECT count(*) FROM {DirectDispatchTests.Database.Table} WHERE attempts = 0 AND last_error IS NULL"))
            .IsEqualTo(1L);
    }

    [Test]
    [Arguments(-24)]
    [Arguments(24)]
    public async Task Retry_backoff_and_failure_fence_use_database_time(int hours)
    {
        await using var database = new DirectDispatchTests.Database();
        await database.Start();
        var transport = new Transport { FailFirst = 1 };
        await using var provider = database.Services(transport, enableDispatch: false);
        var row = Record(null);
        await database.Commit(provider, row);
        using var dispatcher = Dispatcher(database, transport, new Clock(DateTimeOffset.UtcNow.AddHours(hours)));
        var before = await DatabaseTime(database);
        await Dispatch(dispatcher, [row], true);
        await using var command = database.Source.CreateCommand($"SELECT attempts, claim_token IS NULL, deliver_at, clock_timestamp() FROM {DirectDispatchTests.Database.Table}");
        await using var reader = await command.ExecuteReaderAsync();
        await Assert.That(await reader.ReadAsync()).IsTrue();
        await Assert.That(reader.GetInt32(0)).IsEqualTo(1);
        await Assert.That(reader.GetBoolean(1)).IsTrue();
        var due = reader.GetFieldValue<DateTimeOffset>(2);
        await Assert.That(due >= before.AddSeconds(1) && due <= reader.GetFieldValue<DateTimeOffset>(3).AddSeconds(1)).IsTrue();
    }

    private static PostgreSqlDirectDispatcher Dispatcher(DirectDispatchTests.Database database, Transport transport, TimeProvider clock, bool retainSent = false) =>
        new(database.Source, new() { Schema = DirectDispatchTests.Database.Schema, TablePrefix = "test_", RetainSentMessages = retainSent },
            new() { ClaimDuration = TimeSpan.FromMinutes(1) }, [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance,
            timeProvider: clock);

    private static async Task Dispatch(PostgreSqlDirectDispatcher dispatcher, OutboxRecord[] rows, bool recovery)
    {
        if (recovery) await dispatcher.DispatchRecoveredAsync(rows, default);
        else
        {
            var buffered = new List<BufferedOutboxMessage>();
            foreach (var row in rows) { var item = new BufferedOutboxMessage(); item.Copy(row, 32); buffered.Add(item); }
            dispatcher.Committed(buffered, buffered.Count);
            await dispatcher.DispatchQueuedAsync(default);
        }
    }

    private static OutboxRecord Record(DateTimeOffset? deliverAt) =>
        new(Guid.CreateVersion7(), "events", "dispatch.probe;v=1", new("destination", new byte[] { 1 }, default, null, deliverAt), DateTimeOffset.UtcNow);

    private static async Task<DateTimeOffset> DatabaseTime(DirectDispatchTests.Database database)
    {
        await using var command = database.Source.CreateCommand("SELECT clock_timestamp()");
        return new((DateTime)(await command.ExecuteScalarAsync())!);
    }

    private sealed class Clock(DateTimeOffset utcNow) : TimeProvider
    {
        internal DateTimeOffset UtcNow { get; set; } = utcNow;
        internal long AdvanceOnRead { get; init; }
        private long _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override DateTimeOffset GetUtcNow() => UtcNow;
        public override long GetTimestamp() => Interlocked.Add(ref _timestamp, AdvanceOnRead);
    }
}
