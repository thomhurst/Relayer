using System.Buffers;
using Npgsql;
using NpgsqlTypes;
using Relayer.TestFixtures;

namespace Relayer.Outbox.PostgreSql.Tests;

[InheritsTests]
public sealed class SharedMaintenanceContractTests : OutboxMaintenanceContractTests
{
    protected override async Task<MaintenanceFixture> CreateFixtureAsync()
    {
        var fixture = new Fixture();
        try { await fixture.Start(); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    private sealed class Fixture : MaintenanceFixture
    {
        private readonly PostgreSqlFixture _database = new();
        private NpgsqlDataSource? _source;

        public async Task Start()
        {
            await _database.StartAsync();
            _source = NpgsqlDataSource.Create(_database.ConnectionString);
            await PostgreSqlOutboxStoreTests.CreateSchema(_source);
        }

        public override MaintenanceWorker Worker(RecordingTransport transport, int batchSize = 1,
            bool deadRetention = true, bool inboxRetention = true, int cleanupBatchLimit = 10, Action? failure = null, TimeProvider? idleTimeProvider = null) => new WorkerAdapter(new(_source!, new()
            {
                BatchSize = batchSize, CleanupBatchLimit = cleanupBatchLimit,
                SweepInterval = TimeSpan.FromMilliseconds(1), CleanupInterval = TimeSpan.FromHours(1),
                RecoveryDelay = TimeSpan.FromHours(1),
                DeadRetention = deadRetention ? TimeSpan.FromDays(30) : null,
                InboxRetention = inboxRetention ? TimeSpan.FromDays(7) : null
            }, [transport], new FailureLogger<PostgreSqlMaintenanceWorker>(failure), idleTimeProvider: idleTimeProvider));

        public override async Task Insert(Guid id, DateTimeOffset createdAt, int? lane = null,
            DateTimeOffset? deliverAt = null, string status = "pending", DateTimeOffset? claimExpiresAt = null)
        {
            var headers = new ArrayBufferWriter<byte>();
            OutboxHeaderCodec.Write(headers, [new("ce_id", new(id.ToString()))]);
            await using var command = _source!.CreateCommand("""
                INSERT INTO relayer_outbox (id, lane, transport, destination, message_type, headers, body,
                    created_at, deliver_at, status, claim_token, claim_expires_at)
                VALUES (@id, @lane, 'events', 'orders', 'order.placed;v=1', @headers, @body,
                    @created, @due, @status, @token, @expires)
                """);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("lane", NpgsqlDbType.Integer, (object?)lane ?? DBNull.Value);
            command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
            command.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
            command.Parameters.AddWithValue("created", createdAt.ToUniversalTime());
            command.Parameters.AddWithValue("due", NpgsqlDbType.TimestampTz, (object?)deliverAt?.ToUniversalTime() ?? DBNull.Value);
            command.Parameters.AddWithValue("status", status);
            command.Parameters.AddWithValue("token", NpgsqlDbType.Uuid, claimExpiresAt is null ? DBNull.Value : Guid.NewGuid());
            command.Parameters.AddWithValue("expires", NpgsqlDbType.TimestampTz, (object?)claimExpiresAt?.ToUniversalTime() ?? DBNull.Value);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task InsertDead(Guid id, DateTimeOffset failedAt)
        {
            await using var command = _source!.CreateCommand("""
                INSERT INTO relayer_outbox_dead (id, transport, destination, message_type, headers, body, created_at, failed_at)
                VALUES (@id, 'events', 'orders', 'order.placed;v=1', @headers, @body, @failed, @failed)
                """);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("headers", Array.Empty<byte>());
            command.Parameters.AddWithValue("body", Array.Empty<byte>());
            command.Parameters.AddWithValue("failed", failedAt.ToUniversalTime());
            await command.ExecuteNonQueryAsync();
        }

        public override async Task InsertInbox(string consumer, Guid id, DateTimeOffset processedAt)
        {
            await using var command = _source!.CreateCommand("INSERT INTO relayer_inbox (consumer, message_id, processed_at) VALUES (@consumer, @id, @processed)");
            command.Parameters.AddWithValue("consumer", consumer);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("processed", processedAt.ToUniversalTime());
            await command.ExecuteNonQueryAsync();
        }

        public override async Task DropInboxTable()
        {
            await using var command = _source!.CreateCommand("DROP TABLE relayer_inbox");
            await command.ExecuteNonQueryAsync();
        }

        public override async Task MakeDueAndExpireClaims()
        {
            await using var command = _source!.CreateCommand("""
                UPDATE relayer_outbox SET deliver_at = clock_timestamp() - interval '1 second' WHERE deliver_at IS NOT NULL;
                UPDATE relayer_outbox SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE claim_token IS NOT NULL
                """);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task<int> Attempts(Guid id)
        {
            await using var command = _source!.CreateCommand("SELECT attempts FROM relayer_outbox WHERE id = @id");
            command.Parameters.AddWithValue("id", id);
            return (int)(await command.ExecuteScalarAsync())!;
        }

        public override async Task<long> Count(Table table)
        {
            var name = table switch
            {
                Table.Outbox => "relayer_outbox",
                Table.Dead => "relayer_outbox_dead",
                Table.Inbox => "relayer_inbox",
                _ => throw new ArgumentOutOfRangeException(nameof(table))
            };
            await using var command = _source!.CreateCommand($"SELECT count(*) FROM {name}");
            return (long)(await command.ExecuteScalarAsync())!;
        }

        public override async ValueTask DisposeAsync()
        {
            if (_source is not null) await _source.DisposeAsync();
            await _database.DisposeAsync();
        }
    }

    private sealed class WorkerAdapter(PostgreSqlMaintenanceWorker worker) : MaintenanceWorker
    {
        public override ValueTask<int> SweepAsync(CancellationToken cancellationToken = default) => worker.SweepAsync(cancellationToken);
        public override bool CleanupPending => worker.CleanupPending;
        public override ValueTask<int> CleanupAsync(CancellationToken cancellationToken = default) => worker.CleanupAsync(cancellationToken);
        public override ValueTask ReleaseAsync(CancellationToken cancellationToken = default) => worker.ReleaseAsync(cancellationToken);
        public override Task StartAsync(CancellationToken cancellationToken = default) => worker.StartAsync(cancellationToken);
        public override Task StopAsync(CancellationToken cancellationToken = default) => worker.StopAsync(cancellationToken);
        public override void Dispose() => worker.Dispose();
    }
}
