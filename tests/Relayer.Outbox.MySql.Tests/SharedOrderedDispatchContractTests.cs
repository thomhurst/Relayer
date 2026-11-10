using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using Relayer.TestFixtures;

namespace Relayer.Outbox.MySql.Tests;

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
        private readonly MySqlFixture _database = new();
        private MySqlDataSource? _source;

        public async Task Start()
        {
            await _database.StartAsync();
            _source = _database.Source;
        }

        public override OrderedWorker Worker(RecordingTransport transport) => new WorkerAdapter(
            new(_source!, new() { Lanes = 1 }, [transport], NullLogger<MySqlOrderedDispatcher>.Instance));

        public override async Task Insert(Guid id, int? lane, string orderingKey = "customer-Ã©")
        {
            var headers = new ArrayBufferWriter<byte>();
            DurableOutboxHeaderCodec.Write(headers, [new("ce_id", new(id.ToString()))]);
            await using var command = _source!.CreateCommand("""
                INSERT INTO relayer_outbox (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at)
                VALUES (@id, @lane, @key, 'events', 'orders', 'order.placed;v=1', @headers, @body, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
                """);
            command.Parameters.Add(new MySqlParameter("id", id.ToByteArray(bigEndian: true)));
            command.Parameters.Add(new MySqlParameter("lane", (object?)lane ?? DBNull.Value));
            command.Parameters.Add(new MySqlParameter("key", orderingKey));
            command.Parameters.Add(new MySqlParameter("headers", headers.WrittenMemory.ToArray()));
            command.Parameters.Add(new MySqlParameter("body", new byte[] { 7, 8, 9 }));
            await command.ExecuteNonQueryAsync();
        }

        public override async Task SetRouting(Guid id, string transport = "events", string? orderingKey = "customer-Ã©", string destination = "orders")
        {
            await using var command = _source!.CreateCommand("UPDATE relayer_outbox SET transport = @transport, ordering_key = @key, destination = @destination WHERE id = @id");
            command.Parameters.Add(new MySqlParameter("id", id.ToByteArray(bigEndian: true)));
            command.Parameters.Add(new MySqlParameter("transport", transport));
            command.Parameters.Add(new MySqlParameter("key", (object?)orderingKey ?? DBNull.Value));
            command.Parameters.Add(new MySqlParameter("destination", destination));
            await command.ExecuteNonQueryAsync();
        }

        public override async Task SetHeadState(Guid id, HeadState state)
        {
            var change = state switch
            {
                HeadState.Future => "deliver_at = TIMESTAMPADD(DAY, 1, UTC_TIMESTAMP(6))",
                HeadState.Claimed => "claim_token = UUID_TO_BIN(UUID()), claim_expires_at = TIMESTAMPADD(DAY, 1, UTC_TIMESTAMP(6))",
                HeadState.Parked => "status = 'parked'",
                _ => throw new ArgumentOutOfRangeException(nameof(state))
            };
            await using var command = _source!.CreateCommand($"UPDATE relayer_outbox SET {change} WHERE id = @id");
            command.Parameters.Add(new MySqlParameter("id", id.ToByteArray(bigEndian: true)));
            await command.ExecuteNonQueryAsync();
        }

        public override async Task MakeReady(Guid id)
        {
            await using var command = _source!.CreateCommand("UPDATE relayer_outbox SET status = 'pending', deliver_at = UTC_TIMESTAMP(6), claim_token = NULL, claim_expires_at = NULL WHERE id = @id");
            command.Parameters.Add(new MySqlParameter("id", id.ToByteArray(bigEndian: true)));
            await command.ExecuteNonQueryAsync();
        }

        public override async Task<Row[]> ReadRows()
        {
            await using var command = _source!.CreateCommand("SELECT id, attempts, last_error, claim_token, claim_expires_at FROM relayer_outbox ORDER BY id");
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<Row>();
            var id = new byte[16];
            while (await reader.ReadAsync())
            {
                reader.GetBytes(0, 0, id, 0, 16);
                rows.Add(new(new Guid(id, bigEndian: true), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2), !reader.IsDBNull(3), !reader.IsDBNull(4)));
            }
            return rows.ToArray();
        }

        public override async ValueTask DisposeAsync()
        {
            await _database.DisposeAsync();
        }
    }

    private sealed class WorkerAdapter(MySqlOrderedDispatcher worker) : OrderedWorker
    {
        public override ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken = default) => worker.DispatchLaneAsync(lane, cancellationToken);
        public override void Dispose() => worker.Dispose();
    }
}
