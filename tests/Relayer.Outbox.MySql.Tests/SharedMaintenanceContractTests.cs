using System.Buffers;
using MySqlConnector;
using Microsoft.Extensions.Logging.Abstractions;
using Relayer.TestFixtures;

namespace Relayer.Outbox.MySql.Tests;

[InheritsTests]
public sealed class SharedMaintenanceContractTests : OutboxMaintenanceContractTests
{
    protected override async Task<MaintenanceFixture> CreateFixtureAsync()
    {
        var database = new MySqlFixture();
        try { await database.StartAsync(); return new Fixture(database); }
        catch { await database.DisposeAsync(); throw; }
    }

    private sealed class Fixture(MySqlFixture data) : MaintenanceFixture
    {
        private string Outbox => MySqlOutboxSchema.Table(data.Options);
        private string Dead => MySqlOutboxSchema.DeadTable(data.Options);
        private string Inbox => MySqlOutboxSchema.InboxTable(data.Options);
        public override MaintenanceWorker Worker(RecordingTransport transport, int batchSize = 1,
            bool deadRetention = true, bool inboxRetention = true) => new WorkerAdapter(new(data.Source, new()
            {
                BatchSize = batchSize, RecoveryDelay = TimeSpan.FromHours(1),
                DeadRetention = deadRetention ? TimeSpan.FromDays(30) : null,
                InboxRetention = inboxRetention ? TimeSpan.FromDays(7) : null
            }, [transport], NullLogger<MySqlMaintenanceWorker>.Instance));

        public override async Task Insert(Guid id, DateTimeOffset createdAt, int? lane = null,
            DateTimeOffset? deliverAt = null, string status = "pending", DateTimeOffset? claimExpiresAt = null)
        {
            await data.Insert(id, createdAt, lane, deliverAt, status, claimExpiresAt);
            var headers = new ArrayBufferWriter<byte>();
            DurableOutboxHeaderCodec.Write(headers, [new("ce_id", new(id.ToString()))]);
            await using var command = data.Source.CreateCommand($"UPDATE {Outbox} SET headers=@headers,body=X'070809' WHERE id=@id");
            command.Parameters.Add(new MySqlParameter("headers", headers.WrittenMemory));
            command.Parameters.Add(new MySqlParameter("id", id.ToByteArray(bigEndian: true)));
            await command.ExecuteNonQueryAsync();
        }
        public override async Task InsertDead(Guid id, DateTimeOffset failedAt)
        {
            await using var command = data.Source.CreateCommand($"INSERT INTO {Dead} (id,transport,destination,message_type,headers,body,created_at,attempts,failed_at) VALUES (@id,'events','orders','order',X'',X'',@at,1,@at)");
            command.Parameters.Add(new MySqlParameter("id", id.ToByteArray(bigEndian: true)));
            command.Parameters.Add(new MySqlParameter("at", failedAt));
            await command.ExecuteNonQueryAsync();
        }
        public override async Task InsertInbox(string consumer, Guid id, DateTimeOffset processedAt)
        {
            await using var command = data.Source.CreateCommand($"INSERT INTO {Inbox} (consumer,message_id,processed_at) VALUES (@consumer,@id,@at)");
            command.Parameters.Add(new MySqlParameter("consumer", System.Text.Encoding.UTF8.GetBytes(consumer)));
            command.Parameters.Add(new MySqlParameter("id", id.ToByteArray(bigEndian: true)));
            command.Parameters.Add(new MySqlParameter("at", processedAt));
            await command.ExecuteNonQueryAsync();
        }
        public override Task MakeDueAndExpireClaims() => data.Execute($"UPDATE {Outbox} SET deliver_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE deliver_at IS NOT NULL; UPDATE {Outbox} SET claim_expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE claim_token IS NOT NULL");
        public override async Task<int> Attempts(Guid id) => Convert.ToInt32(await Scalar($"SELECT attempts FROM {Outbox} WHERE id=X'{Convert.ToHexString(id.ToByteArray(bigEndian: true))}'"));
        public override async Task<long> Count(Table table) => Convert.ToInt64(await Scalar($"SELECT COUNT(*) FROM {table switch { Table.Outbox => Outbox, Table.Dead => Dead, Table.Inbox => Inbox, _ => throw new ArgumentOutOfRangeException(nameof(table)) }}"));
        private async Task<object?> Scalar(string sql)
        {
            await using var command = data.Source.CreateCommand(sql);
            return await command.ExecuteScalarAsync();
        }
        public override ValueTask DisposeAsync() => data.DisposeAsync();
    }

    private sealed class WorkerAdapter(MySqlMaintenanceWorker worker) : MaintenanceWorker
    {
        public override ValueTask<int> SweepAsync(CancellationToken cancellationToken = default) => worker.SweepAsync(cancellationToken);
        public override ValueTask<int> CleanupAsync(CancellationToken cancellationToken = default) => worker.CleanupAsync(cancellationToken);
        public override ValueTask ReleaseAsync(CancellationToken cancellationToken = default) => worker.ReleaseAsync(cancellationToken);
        public override void Dispose() => worker.Dispose();
    }
}
