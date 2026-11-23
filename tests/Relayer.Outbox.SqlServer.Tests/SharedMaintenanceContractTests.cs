using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using Relayer.TestFixtures;
using Relayer.Transports;

namespace Relayer.Outbox.SqlServer.Tests;

[InheritsTests]
[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class SharedMaintenanceContractTests(SqlServerFixture database) : OutboxMaintenanceContractTests
{
    protected override async Task<MaintenanceFixture> CreateFixtureAsync()
    {
        return await CreateFixtureAsync(TimeSpan.FromDays(30));
    }

    [Test]
    public async Task Retention_before_the_datetime2_range_leaves_dead_rows_and_cleans_the_inbox()
    {
        await using var fixture = await CreateFixtureAsync(TimeSpan.MaxValue);
        await fixture.InsertDead(Guid.NewGuid(), DateTimeOffset.UtcNow.AddDays(-60));
        await fixture.InsertInbox("consumer", Guid.NewGuid(), DateTimeOffset.UtcNow.AddDays(-8));
        using var worker = fixture.Worker(new());

        await Assert.That(await worker.CleanupAsync()).IsEqualTo(1);
        await Assert.That(await fixture.Count(Table.Dead)).IsEqualTo(1L);
        await Assert.That(await fixture.Count(Table.Inbox)).IsEqualTo(0L);
    }

    private async Task<MaintenanceFixture> CreateFixtureAsync(TimeSpan deadRetentionPeriod)
    {
        var data = new StoreData(database);
        await data.Create();
        return new Fixture(new SqlConnectionStringBuilder(database.ConnectionString) { InitialCatalog = "master" }.ConnectionString, data, deadRetentionPeriod);
    }

    private sealed class Fixture(string connectionString, StoreData data, TimeSpan deadRetentionPeriod) : MaintenanceFixture
    {
        private string Dead => SqlServerOutboxSchema.DeadTable(data.Options);

        private string Inbox => SqlServerOutboxSchema.InboxTable(data.Options);

        public override MaintenanceWorker Worker(RecordingTransport transport, int batchSize = 1,
            bool deadRetention = true, bool inboxRetention = true, int cleanupBatchLimit = 10, Action? failure = null, TimeProvider? idleTimeProvider = null,
            RelayerInstrumentation? instrumentation = null, TimeSpan? sweepInterval = null, TimeSpan? cleanupInterval = null)
        {
            return new WorkerAdapter(new(connectionString, new()
            {
                TablePrefix = data.Options.TablePrefix,
                BatchSize = batchSize,
                CleanupBatchLimit = cleanupBatchLimit,
                SweepInterval = sweepInterval ?? TimeSpan.FromMilliseconds(1),
                CleanupInterval = cleanupInterval ?? TimeSpan.FromHours(1),
                RecoveryDelay = TimeSpan.FromHours(1),
                DeadRetention = deadRetention ? deadRetentionPeriod : null,
                InboxRetention = inboxRetention ? TimeSpan.FromDays(7) : null
            }, [transport], new FailureLogger<SqlServerMaintenanceWorker>(failure), instrumentation: instrumentation, idleTimeProvider: idleTimeProvider, storeName: "store"));
        }

        public override async Task Insert(Guid id, DateTimeOffset createdAt, int? lane = null,
            DateTimeOffset? deliverAt = null, string status = "pending", DateTimeOffset? claimExpiresAt = null)
        {
            await data.Insert(id, createdAt, lane, deliverAt, status, claimExpiresAt);
            using var headers = new MemoryStream();

            using (var writer = new BinaryWriter(headers, Encoding.UTF8, leaveOpen: true))
            {
                writer.Write(1);
                writer.Write(1);
                writer.Write(5);
                writer.Write(Encoding.UTF8.GetBytes("ce_id"));
                var value = Encoding.UTF8.GetBytes(id.ToString());
                writer.Write(value.Length);
                writer.Write(value);
            }

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"UPDATE {data.Table} SET headers=@headers, body=0x070809 WHERE id=@id", connection);
            command.Parameters.Add("@headers", SqlDbType.VarBinary, -1).Value = headers.ToArray();
            command.Parameters.Add("@id", SqlDbType.Binary, 16).Value = id.ToByteArray(bigEndian: true);
            await command.ExecuteNonQueryAsync();
        }

        public override Task InsertDead(Guid id, DateTimeOffset failedAt)
        {
            return data.Execute($"""
            INSERT INTO {Dead} (id, transport, destination, message_type, headers, body, created_at, attempts, failed_at)
                VALUES (0x{Convert.ToHexString(id.ToByteArray(bigEndian: true))}, 'events', 'orders', 'order', 0x, 0x, '{failedAt:O}', 1, '{failedAt:O}')
            """);
        }

        public override Task InsertInbox(string consumer, Guid id, DateTimeOffset processedAt)
        {
            return data.Execute($"INSERT INTO {Inbox} (consumer, message_id, processed_at) VALUES ('{consumer}', '{id}', '{processedAt:O}')");
        }

        public override Task DropInboxTable()
        {
            return data.Execute($"DROP TABLE {Inbox}");
        }

        public override Task MakeDueAndExpireClaims()
        {
            return data.Execute($"UPDATE {data.Table} SET deliver_at=DATEADD(second,-1,SYSUTCDATETIME()) WHERE deliver_at IS NOT NULL; UPDATE {data.Table} SET claim_expires_at=DATEADD(second,-1,SYSUTCDATETIME()) WHERE claim_token IS NOT NULL");
        }

        public override async Task<int> Attempts(Guid id)
        {
            return (int)(await Scalar($"SELECT attempts FROM {data.Table} WHERE id=0x{Convert.ToHexString(id.ToByteArray(bigEndian: true))}"))!;
        }

        public override async Task<long> Count(Table table)
        {
            return (long)(await Scalar($"SELECT COUNT_BIG(*) FROM {table switch { Table.Outbox => data.Table, Table.Dead => Dead, Table.Inbox => Inbox, _ => throw new ArgumentOutOfRangeException(nameof(table)) }}"))!;
        }

        private async Task<object?> Scalar(string sql)
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(sql, connection);
            return await command.ExecuteScalarAsync();
        }

        public override ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class WorkerAdapter(SqlServerMaintenanceWorker worker) : MaintenanceWorker
    {
        public override ValueTask<int> SweepAsync(CancellationToken cancellationToken = default)
        {
            return worker.SweepAsync(cancellationToken);
        }

        public override bool CleanupPending => worker.CleanupPending;

        public override ValueTask<int> CleanupAsync(CancellationToken cancellationToken = default)
        {
            return worker.CleanupAsync(cancellationToken);
        }

        public override ValueTask ReleaseAsync(CancellationToken cancellationToken = default)
        {
            return worker.ReleaseAsync(cancellationToken);
        }

        public override Task StartAsync(CancellationToken cancellationToken = default)
        {
            return worker.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken = default)
        {
            return worker.StopAsync(cancellationToken);
        }

        public override ValueTask RefreshDepthAsync(CancellationToken cancellationToken = default)
        {
            return worker.RefreshDepthAsync(cancellationToken);
        }

        public override void Dispose()
        {
            worker.Dispose();
        }
    }
}
