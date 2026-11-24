using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer.Transports;
using Transport = Relayer.TestFixtures.OutboxDirectDispatchContractTests.RecordingTransport;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public class DispatchClockTests(SqlServerFixture database)
{
    private string ConnectionString => new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" }.ConnectionString;

    [Test]
    [Arguments(-2)]
    [Arguments(0)]
    [Arguments(2)]
    public async Task Direct_dispatch_claims_due_rows_and_leaves_future_rows_pending(int offsetHours)
    {
        var data = new StoreData(database);
        await data.Create();
        var now = await DatabaseTimeAsync();
        var due = await InsertAsync(data, now, now.AddMinutes(-1));
        var future = await InsertAsync(data, now, now.AddHours(1));
        var transport = new Transport();
        using var worker = new SqlServerDirectDispatcher(ConnectionString, data.Options, new(),
            [transport], NullLogger<SqlServerDirectDispatcher>.Instance, timeProvider: new Clock(now.AddHours(offsetHours)));

        await DispatchAsync(worker, due, future);

        await Assert.That(transport.Received.Count).IsEqualTo(1);
        await Assert.That(await CountAsync(data, "claim_token IS NULL AND deliver_at > SYSUTCDATETIME()")).IsEqualTo(1L);
        await Assert.That(await CountAsync(data, "1 = 1")).IsEqualTo(1L);

        await data.Execute($"UPDATE {data.Table} SET deliver_at = DATEADD(second, -1, SYSUTCDATETIME())");
        await DispatchAsync(worker, future);

        await Assert.That(transport.Received.Count).IsEqualTo(2);
        await Assert.That(await CountAsync(data, "1 = 1")).IsEqualTo(0L);
    }

    [Test]
    [Arguments(-2)]
    [Arguments(0)]
    [Arguments(2)]
    public async Task Recovery_dispatches_a_due_row_in_the_sweep_that_selects_it(int offsetHours)
    {
        var data = new StoreData(database);
        await data.Create();
        var now = await DatabaseTimeAsync();
        // The database sees both a due schedule and an expired claim.
        await InsertAsync(data, now, now.AddMinutes(-1), now.AddMinutes(-1));
        await InsertAsync(data, now, now.AddHours(1));
        var transport = new Transport();
        using var worker = new SqlServerMaintenanceWorker(ConnectionString,
            new()
            {
                TablePrefix = data.Options.TablePrefix,
                RecoveryDelay = TimeSpan.Zero
            },
            [transport], NullLogger<SqlServerMaintenanceWorker>.Instance, idleTimeProvider: new Clock(now.AddHours(offsetHours)));

        try
        {
            await Assert.That(await worker.SweepAsync()).IsEqualTo(1);
            await Assert.That(transport.Received.Count).IsEqualTo(1);
            await Assert.That(await CountAsync(data, "claim_token IS NULL AND deliver_at > SYSUTCDATETIME()")).IsEqualTo(1L);
            await Assert.That(await CountAsync(data, "1 = 1")).IsEqualTo(1L);
        }
        finally
        {
            await worker.ReleaseAsync();
        }
    }

    [Test]
    [Arguments(30, 12_345L)]
    [Arguments(1_000_000, 0L)]
    public async Task Database_claim_accepts_durations_beyond_the_millisecond_offset_limit(int days, long ticks)
    {
        var data = new StoreData(database);
        await data.Create();
        var now = await DatabaseTimeAsync();
        var row = await InsertAsync(data, now, now.AddMinutes(-1));
        var claim = new OutboxClaim(row.Id, Guid.NewGuid());
        var store = data.Store();
        var duration = TimeSpan.FromDays(days) + TimeSpan.FromTicks(ticks);
        var sql = store.CreateDispatchClaimSql(duration);

        await Assert.That(await store.ClaimForDispatchAsync(claim, sql, default)).IsTrue();
        var after = await DatabaseTimeAsync();

        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT claim_expires_at FROM {data.Table}", connection);
        var expiry = (DateTimeOffset)(await command.ExecuteScalarAsync())!;
        await Assert.That(expiry >= now + duration && expiry <= after + duration).IsTrue();
    }

    private static async Task<OutboxRecord> InsertAsync(StoreData data, DateTimeOffset now, DateTimeOffset due, DateTimeOffset? expires = null)
    {
        var id = Guid.CreateVersion7();
        var created = now.AddMinutes(-5);
        await data.Insert(id, created, deliverAt: due, expires: expires);
        return new(id, "events", "order.placed.v1", new("orders", new byte[] { 1 }, default, null, due), created);
    }

    private static async Task DispatchAsync(SqlServerDirectDispatcher worker, params OutboxRecord[] records)
    {
        var buffers = new List<SqlServerBufferedMessage>(records.Length);

        foreach (var record in records)
        {
            var buffer = new SqlServerBufferedMessage();
            buffer.Copy(record, 1, unordered: true);
            buffers.Add(buffer);
        }

        worker.Committed(buffers, buffers.Count);
        await worker.DispatchQueuedAsync(default);
    }

    private async Task<DateTimeOffset> DatabaseTimeAsync()
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand("SELECT CAST(SYSUTCDATETIME() AS datetimeoffset(7))", connection);
        return (DateTimeOffset)(await command.ExecuteScalarAsync())!;
    }

    private async Task<long> CountAsync(StoreData data, string predicate)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"SELECT COUNT_BIG(*) FROM {data.Table} WHERE {predicate}", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private sealed class Clock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow()
        {
            return now;
        }
    }
}
