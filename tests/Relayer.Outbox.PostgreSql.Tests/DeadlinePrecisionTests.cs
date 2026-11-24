using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using Relayer.Transports;
using Transport = Relayer.TestFixtures.OutboxDirectDispatchContractTests.RecordingTransport;

namespace Relayer.Outbox.PostgreSql.Tests;

public class DeadlinePrecisionTests
{
    [Test]
    [Arguments(10_000_000L, 10_000_000L)]
    [Arguments(10_000_009L, 10_000_010L)]
    public async Task Ordered_membership_lane_and_claim_use_rounded_deadlines(long ticks, long expectedTicks)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = FrozenClockSource(fixture);
        await InitializeAsync(source);
        var duration = TimeSpan.FromTicks(ticks);
        var coordinator = new PostgreSqlLaneCoordinator(source, new()
        {
            Lanes = 1,
            LeaseDuration = duration
        });
        using var lane = new PostgreSqlOrderedLane(source, new(), duration);
        var id = Guid.CreateVersion7();
        await InsertAsync(source, id, 0);
        await coordinator.HeartbeatAsync(default);

        await using var connection = await source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await Assert.That(await coordinator.AcquireAsync(connection, transaction, 0, default)).IsTrue();
        await Assert.That(await lane.ClaimAsync(connection, transaction, new(id, Guid.NewGuid()), default)).IsTrue();
        await using var command = new NpgsqlCommand("""
            SELECT expires_at - clock_timestamp() FROM relayer_leases ORDER BY name;
            SELECT claim_expires_at - clock_timestamp() FROM relayer_outbox;
            """, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync();
        var expected = TimeSpan.FromTicks(expectedTicks);
        await AssertNextDurationAsync(reader, expected);
        await AssertNextDurationAsync(reader, expected);
        await Assert.That(await reader.ReadAsync()).IsFalse();
        await Assert.That(await reader.NextResultAsync()).IsTrue();
        await AssertNextDurationAsync(reader, expected);
    }

    [Test]
    [Arguments(10_000_000L, 10_000_000L)]
    [Arguments(10_000_009L, 10_000_010L)]
    public async Task Maintenance_acquisition_and_renewal_use_rounded_deadlines(long ticks, long expectedTicks)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = FrozenClockSource(fixture);
        await InitializeAsync(source);

        await using (var audit = source.CreateCommand("""
            CREATE TABLE lease_deadlines (name text, operation text, duration interval);
            CREATE FUNCTION audit_lease_deadline() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO lease_deadlines VALUES (NEW.name, TG_OP, NEW.expires_at - clock_timestamp());
                RETURN NEW;
            END;
            $$;
            CREATE TRIGGER audit_lease_deadline AFTER INSERT OR UPDATE ON relayer_leases
            FOR EACH ROW EXECUTE FUNCTION audit_lease_deadline();
            """))
        {
            await audit.ExecuteNonQueryAsync();
        }

        using var worker = new PostgreSqlMaintenanceWorker(source, new()
        {
            LeaseDuration = TimeSpan.FromTicks(ticks),
            DeadRetention = TimeSpan.FromDays(1),
            InboxRetention = null
        }, [], NullLogger<PostgreSqlMaintenanceWorker>.Instance);
        await worker.SweepAsync(default);
        await worker.CleanupAsync(default);

        await using var command = source.CreateCommand("SELECT duration, name, operation FROM lease_deadlines ORDER BY name, operation");
        await using var reader = await command.ExecuteReaderAsync();
        var expected = TimeSpan.FromTicks(expectedTicks);

        foreach (var name in new[] { "cleanup", "sweeper" })
        {
            foreach (var operation in new[] { "INSERT", "UPDATE" })
            {
                await AssertNextDurationAsync(reader, expected);
                await Assert.That(reader.GetString(1)).IsEqualTo(name);
                await Assert.That(reader.GetString(2)).IsEqualTo(operation);
            }
        }

        await Assert.That(await reader.ReadAsync()).IsFalse();
    }

    [Test]
    [Arguments(19L, 20L, false)]
    [Arguments(20L, 20L, false)]
    [Arguments(TimeSpan.TicksPerDay - 1, TimeSpan.TicksPerDay, false)]
    [Arguments(TimeSpan.TicksPerDay, TimeSpan.TicksPerDay, false)]
    [Arguments(19L, 20L, true)]
    [Arguments(20L, 20L, true)]
    public async Task Direct_and_deferred_claims_use_rounded_deadlines(long ticks, long expectedTicks, bool deferred)
    {
        await using var fixture = new PostgreSqlFixture();
        await fixture.StartAsync();
        await using var source = FrozenClockSource(fixture);
        await InitializeAsync(source);
        var id = Guid.CreateVersion7();
        await InsertAsync(source, id, null);
        var duration = TimeSpan.FromTicks(ticks);

        if (deferred)
        {
            var store = new PostgreSqlOutboxStore(source, new());
            var claimed = new bool[1];
            await store.ClaimDirectBatchAsync(new[] { new OutboxClaim(id, Guid.NewGuid()) }, claimed,
                PostgreSqlInterval.ElapsedDuration(duration), default, deferred: true);
            await Assert.That(claimed[0]).IsTrue();
            await AssertStoredClaimDurationAsync(source, TimeSpan.FromTicks(expectedTicks));
            return;
        }

        var transport = new Transport { Block = true };
        using var dispatcher = new PostgreSqlDirectDispatcher(source, new(), new()
        {
            ClaimDuration = duration
        },
            [transport], NullLogger<PostgreSqlDirectDispatcher>.Instance, timeProvider: new FixedMonotonicClock());
        var row = new OutboxRecord(id, "events", "order;v=1", new("orders", new byte[] { 1 }, default), DateTimeOffset.UtcNow);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var dispatch = dispatcher.DispatchRecoveredAsync(new[] { row }, timeout.Token).AsTask();

        try
        {
            await transport.Entered.Task.WaitAsync(timeout.Token);
            await AssertStoredClaimDurationAsync(source, TimeSpan.FromTicks(expectedTicks));
        }
        finally
        {
            transport.Release.TrySetResult();
            await dispatch;
        }
    }

    private static NpgsqlDataSource FrozenClockSource(PostgreSqlFixture fixture)
    {
        var settings = new NpgsqlConnectionStringBuilder(fixture.ConnectionString)
        {
            SearchPath = "public, pg_catalog",
            Timezone = "Europe/London"
        };
        return NpgsqlDataSource.Create(settings.ConnectionString);
    }

    private static async Task InitializeAsync(NpgsqlDataSource source)
    {
        await PostgreSqlOutboxStoreTests.CreateSchema(source);
        // Freeze the database clock before the spring DST transition to verify elapsed durations.
        await using var command = source.CreateCommand("""
            CREATE FUNCTION public.clock_timestamp() RETURNS timestamptz
            LANGUAGE sql IMMUTABLE AS $$ SELECT timestamptz '2026-03-28 12:00:00.123456+00' $$;
            """);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task InsertAsync(NpgsqlDataSource source, Guid id, int? lane)
    {
        await using var command = source.CreateCommand("""
            INSERT INTO relayer_outbox (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at)
            VALUES (@id, @lane, 'key', 'events', 'orders', 'order;v=1', ''::bytea, ''::bytea, clock_timestamp());
            """);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.Add(new NpgsqlParameter<int?>("lane", NpgsqlDbType.Integer) { TypedValue = lane });
        await command.ExecuteNonQueryAsync();
    }

    private static async Task AssertStoredClaimDurationAsync(NpgsqlDataSource source, TimeSpan expected)
    {
        await using var command = source.CreateCommand("SELECT claim_expires_at - clock_timestamp() FROM relayer_outbox");
        await using var reader = await command.ExecuteReaderAsync();
        await AssertNextDurationAsync(reader, expected);
    }

    private static async Task AssertNextDurationAsync(NpgsqlDataReader reader, TimeSpan expected)
    {
        await Assert.That(await reader.ReadAsync()).IsTrue();
        await Assert.That(reader.GetFieldValue<TimeSpan>(0)).IsEqualTo(expected);
    }

    private sealed class FixedMonotonicClock : TimeProvider
    {
        public override long GetTimestamp()
        {
            return 0;
        }
    }
}
