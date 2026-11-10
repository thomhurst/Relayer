using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Data.SqlClient;
using System.Data;
using Relayer.TestFixtures;

namespace Relayer.Outbox.SqlServer.Tests;

[InheritsTests]
[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class SharedOrderedDispatchContractTests(SqlServerFixture database) : OutboxOrderedDispatchContractTests
{
    protected override async Task<OrderedFixture> CreateFixtureAsync()
    {
        var fixture = new Fixture(database);
        try { await fixture.Start(); return fixture; }
        catch { await fixture.DisposeAsync(); throw; }
    }

    private sealed class Fixture(SqlServerFixture database) : OrderedFixture
    {
        private readonly StoreData _data = new(database);

        public async Task Start()
        {
            await _data.Create();
        }

        public override OrderedWorker Worker(RecordingTransport transport) => new WorkerAdapter(
            new(database.ConnectionString, new() { Lanes = 1, TablePrefix = _data.Options.TablePrefix }, [transport], NullLogger<SqlServerOrderedDispatcher>.Instance));

        public override async Task Insert(Guid id, int? lane, string orderingKey = "customer-Ã©")
        {
            using var headers = new MemoryStream();
            using (var writer = new BinaryWriter(headers, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                writer.Write(1); writer.Write(1);
                writer.Write(5); writer.Write("ce_id"u8);
                var value = System.Text.Encoding.UTF8.GetBytes(id.ToString());
                writer.Write(value.Length); writer.Write(value);
            }
            await using var connection = new SqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"""
                INSERT INTO {_data.Table} (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at)
                VALUES (@id, @lane, @key, 'events', 'orders', 'order.placed;v=1', @headers, @body, SYSUTCDATETIME(), SYSUTCDATETIME())
                """, connection);
            command.Parameters.Add("@id", SqlDbType.Binary, 16).Value = id.ToByteArray(bigEndian: true);
            command.Parameters.Add("@lane", SqlDbType.Int).Value = (object?)lane ?? DBNull.Value;
            command.Parameters.AddWithValue("key", orderingKey);
            command.Parameters.AddWithValue("headers", headers.ToArray());
            command.Parameters.AddWithValue("body", new byte[] { 7, 8, 9 });
            await command.ExecuteNonQueryAsync();
        }

        public override async Task SetRouting(Guid id, string transport = "events", string? orderingKey = "customer-Ã©", string destination = "orders")
        {
            await using var connection = new SqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"UPDATE {_data.Table} SET transport = @transport, ordering_key = @key, destination = @destination WHERE id = @id", connection);
            command.Parameters.Add("@id", SqlDbType.Binary, 16).Value = id.ToByteArray(bigEndian: true);
            command.Parameters.AddWithValue("transport", transport);
            command.Parameters.Add("@key", SqlDbType.NVarChar, -1).Value = (object?)orderingKey ?? DBNull.Value;
            command.Parameters.AddWithValue("destination", destination);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task SetHeadState(Guid id, HeadState state)
        {
            var change = state switch
            {
                HeadState.Future => "deliver_at = DATEADD(day, 1, SYSUTCDATETIME())",
                HeadState.Claimed => "claim_token = CONVERT(binary(16), NEWID()), claim_expires_at = DATEADD(day, 1, SYSUTCDATETIME())",
                HeadState.Parked => "status = 'parked'",
                _ => throw new ArgumentOutOfRangeException(nameof(state))
            };
            await using var connection = new SqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"UPDATE {_data.Table} SET {change} WHERE id = @id", connection);
            command.Parameters.Add("@id", SqlDbType.Binary, 16).Value = id.ToByteArray(bigEndian: true);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task MakeReady(Guid id)
        {
            await using var connection = new SqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"UPDATE {_data.Table} SET status = 'pending', deliver_at = SYSUTCDATETIME(), claim_token = NULL, claim_expires_at = NULL WHERE id = @id", connection);
            command.Parameters.Add("@id", SqlDbType.Binary, 16).Value = id.ToByteArray(bigEndian: true);
            await command.ExecuteNonQueryAsync();
        }

        public override async Task<Row[]> ReadRows()
        {
            await using var connection = new SqlConnection(database.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand($"SELECT id, attempts, last_error, claim_token, claim_expires_at FROM {_data.Table} ORDER BY id", connection);
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<Row>();
            while (await reader.ReadAsync())
                rows.Add(new(new Guid((byte[])reader.GetValue(0), bigEndian: true), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2), !reader.IsDBNull(3), !reader.IsDBNull(4)));
            return rows.ToArray();
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;

    }

    private sealed class WorkerAdapter(SqlServerOrderedDispatcher worker) : OrderedWorker
    {
        public override ValueTask<int> DispatchLaneAsync(int lane, CancellationToken cancellationToken = default) => worker.DispatchLaneAsync(lane, cancellationToken);
        public override void Dispose() => worker.Dispose();
    }
}
