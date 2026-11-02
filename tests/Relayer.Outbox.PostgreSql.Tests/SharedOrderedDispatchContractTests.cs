using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using Relayer.TestFixtures;

namespace Relayer.Outbox.PostgreSql.Tests;

[InheritsTests]
public sealed class SharedOrderedDispatchContractTests : OutboxOrderedDispatchContractTests
{
    protected override async Task<OrderedFixture> CreateFixtureAsync()
    {
        var fixture = new Fixture();
        try { await fixture.Start(); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    private sealed class Fixture : OrderedFixture
    {
        private readonly PostgreSqlFixture _database = new();
        private NpgsqlDataSource? _source;

        public async Task Start()
        {
            await _database.StartAsync();
            _source = NpgsqlDataSource.Create(_database.ConnectionString);
            await PostgreSqlOutboxStoreTests.CreateSchema(_source);
        }

        public override OrderedWorker Worker(RecordingTransport transport) => new WorkerAdapter(
            new(_source!, new() { Lanes = 1 }, [transport], NullLogger<PostgreSqlOrderedDispatcher>.Instance));

        public override async Task Insert(Guid id, int? lane, string orderingKey = "customer-Ã©")
        {
            var headers = new ArrayBufferWriter<byte>();
            OutboxHeaderCodec.Write(headers, [new("ce_id", new(id.ToString()))]);
            await using var command = _source!.CreateCommand("""
                INSERT INTO relayer_outbox (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at)
                VALUES (@id, @lane, @key, 'events', 'orders', 'order.placed;v=1', @headers, @body, clock_timestamp(), clock_timestamp())
                """);
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("lane", NpgsqlDbType.Integer, (object?)lane ?? DBNull.Value);
            command.Parameters.AddWithValue("key", orderingKey);
            command.Parameters.AddWithValue("headers", headers.WrittenMemory.ToArray());
            command.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
            await command.ExecuteNonQueryAsync();
        }

        public override async Task SetRouting(Guid id, string transport = "events", string? orderingKey = "customer-Ã©", string destination = "orders")
        {
            await using var command = _source!.CreateCommand("UPDATE relayer_outbox SET transport = @transport, ordering_key = @key, destination = @destination WHERE id = @id");
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("transport", transport);
            command.Parameters.AddWithValue("key", NpgsqlDbType.Text, (object?)orderingKey ?? DBNull.Value);
            command.Parameters.AddWithValue("destination", destination);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task SetHeadState(Guid id, HeadState state)
        {
            var change = state switch
            {
                HeadState.Future => "deliver_at = clock_timestamp() + interval '1 day'",
                HeadState.Claimed => "claim_token = gen_random_uuid(), claim_expires_at = clock_timestamp() + interval '1 day'",
                HeadState.Parked => "status = 'parked'",
                _ => throw new ArgumentOutOfRangeException(nameof(state))
            };
            await using var command = _source!.CreateCommand($"UPDATE relayer_outbox SET {change} WHERE id = @id");
            command.Parameters.AddWithValue("id", id);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task MakeReady(Guid id)
        {
            await using var command = _source!.CreateCommand("UPDATE relayer_outbox SET status = 'pending', deliver_at = clock_timestamp(), claim_token = NULL, claim_expires_at = NULL WHERE id = @id");
            command.Parameters.AddWithValue("id", id);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task<Row[]> ReadRows()
        {
            await using var command = _source!.CreateCommand("SELECT id, attempts, last_error, claim_token, claim_expires_at FROM relayer_outbox ORDER BY id");
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<Row>();
            while (await reader.ReadAsync())
                rows.Add(new(reader.GetGuid(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2), !reader.IsDBNull(3), !reader.IsDBNull(4)));
            return rows.ToArray();
        }

        public override async ValueTask DisposeAsync()
        {
            if (_source is not null) await _source.DisposeAsync();
            await _database.DisposeAsync();
        }
    }

    private sealed class WorkerAdapter(PostgreSqlOrderedDispatcher worker) : OrderedWorker
    {
        public override ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken = default) => worker.DispatchLaneAsync(lane, cancellationToken);
        public override void Dispose() => worker.Dispose();
    }
}
