using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class LeasePrecisionTests(SqlServerFixture database)
{
    private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddTicks(1_234_567);

    [Test]
    [Arguments(10_000_000L, 1_000)]
    [Arguments(10_009_999L, 1_001)]
    public async Task Ordered_worker_persists_membership_lane_and_claim_deadlines(long ticks, int milliseconds)
    {
        var data = new StoreData(database);
        await data.Create();
        var id = Guid.CreateVersion7();
        await data.Insert(id, s_now, lane: 0);
        var options = new SqlServerOrderedDispatchOptions
        {
            Lanes = 1,
            TablePrefix = data.Options.TablePrefix,
            LeaseDuration = TimeSpan.FromTicks(ticks),
            PollInterval = TimeSpan.FromMilliseconds(10)
        };
        var coordinator = new SqlServerLaneCoordinator(database.ConnectionString, options);
        using var lane = new SqlServerOrderedLane(database.ConnectionString, data.Options, options.LeaseDuration, 10);
        using var clock = FixClock(data.Options.TablePrefix);
        var leases = SqlServerOutboxSchema.OrderedLeaseTable(data.Options);

        await coordinator.RebalanceAsync(default);
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();

        await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
        {
            await Assert.That(await coordinator.AcquireAsync(connection, transaction, 0, default)).IsTrue();
            await Assert.That(await lane.ClaimAsync(connection, transaction, new(id, Guid.NewGuid()), default)).IsTrue();
            await transaction.CommitAsync();
        }

        await AssertDeadlinesAsync($"""
            SELECT expires_at FROM {leases}
            UNION ALL SELECT claim_expires_at FROM {data.Table}
            """, milliseconds, 3);
    }

    [Test]
    [Arguments(10_000_000L, 1_000)]
    [Arguments(10_009_999L, 1_001)]
    public async Task Maintenance_persists_acquisition_and_renewal_deadlines(long ticks, int milliseconds)
    {
        var data = new StoreData(database);
        await data.Create();
        var leases = SqlServerOutboxSchema.LeaseTable(data.Options);
        var audit = $"[dbo].[{data.Options.TablePrefix}lease_audit]";
        await data.Execute($"CREATE TABLE {audit} (expires_at datetimeoffset(7) NOT NULL)");
        await data.Execute($"""
            CREATE TRIGGER [dbo].[{data.Options.TablePrefix}record_lease] ON {leases} AFTER INSERT, UPDATE AS
            BEGIN
                SET NOCOUNT ON;
                INSERT INTO {audit} (expires_at) SELECT expires_at FROM inserted;
            END;
            """);
        var options = new SqlServerMaintenanceOptions
        {
            TablePrefix = data.Options.TablePrefix,
            LeaseDuration = TimeSpan.FromTicks(ticks)
        };
        var connectionString = new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" };
        using var worker = new SqlServerMaintenanceWorker(connectionString.ConnectionString, options, [],
            NullLogger<SqlServerMaintenanceWorker>.Instance);
        using var clock = FixClock(data.Options.TablePrefix);

        await Assert.That(await worker.SweepAsync()).IsEqualTo(0);

        await AssertDeadlinesAsync($"SELECT expires_at FROM {audit}", milliseconds, 2);
    }

    private async Task AssertDeadlinesAsync(string sql, int milliseconds, int count)
    {
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var actualCount = 0;

        while (await reader.ReadAsync())
        {
            long ticks = (reader.GetDateTimeOffset(0) - s_now).Ticks;
            await Assert.That(ticks).IsEqualTo(milliseconds * TimeSpan.TicksPerMillisecond);
            actualCount++;
        }

        await Assert.That(actualCount).IsEqualTo(count);
    }

    private static IDisposable FixClock(string tablePrefix)
    {
        // Only replace the clock sample in this test's commands. SQL Server still computes and persists each deadline.
        IDisposable? commands = null;
        using var listeners = DiagnosticListener.AllListeners.Subscribe(new Observer<DiagnosticListener>(listener =>
        {
            if (listener.Name == "SqlClientDiagnosticListener")
            {
                commands = listener.Subscribe(new Observer<KeyValuePair<string, object?>>(entry =>
                {
                    if (entry.Value is SqlClientCommandBefore before &&
                        before.Command.CommandText.Contains($"[{tablePrefix}", StringComparison.Ordinal))
                    {
                        before.Command.CommandText = before.Command.CommandText.Replace("SYSUTCDATETIME()",
                            "CAST('2026-01-01T00:00:00.1234567' AS datetime2(7))", StringComparison.Ordinal);
                    }
                }), static name => name == SqlClientCommandBefore.Name);
            }
        }));
        return commands ?? throw new InvalidOperationException("SqlClient diagnostic listener was not initialized.");
    }

    private sealed class Observer<T>(Action<T> onNext) : IObserver<T>
    {
        public void OnNext(T value)
        {
            onNext(value);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }
}
