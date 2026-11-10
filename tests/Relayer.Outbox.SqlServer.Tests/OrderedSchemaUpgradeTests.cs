using System.Data;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class OrderedSchemaUpgradeTests(SqlServerFixture database)
{
    [Test]
    public async Task Explicit_upgrade_preserves_historical_UUID_order_and_continues_native_sequence()
    {
        // Exercise both identifier and SQL literal escaping through the dynamic migration batches.
        var options = new SqlServerOutboxOptions { TablePrefix = "m_]'" + Guid.NewGuid().ToString("N") + "_" };
        var table = SqlServerOutboxSchema.Table(options);
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using (var old = new SqlCommand($"""
            CREATE TABLE {table} (id binary(16) NOT NULL PRIMARY KEY, lane int NULL,
                deliver_at datetimeoffset(7) NULL, claim_expires_at datetimeoffset(7) NULL,
                status varchar(7) NOT NULL DEFAULT 'pending');
            INSERT INTO {table} (id, lane) VALUES (0x01900000000070008000000000000002, 0), (0x01900000000070008000000000000001, 0);
            """, connection))
            await old.ExecuteNonQueryAsync();
        await using (var unguarded = new SqlCommand(SqlServerOutboxSchema.UpgradeOrderedDispatchScript(options), connection))
            await Assert.That(async () => await unguarded.ExecuteNonQueryAsync()).Throws<SqlException>();
        await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
        {
            await using var upgrade = new SqlCommand(SqlServerOutboxSchema.UpgradeOrderedDispatchScript(options), connection, transaction);
            await upgrade.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
        }
        await using (var next = new SqlCommand($"INSERT INTO {table} (id, lane) VALUES (0x01900000000070008000000000000000, 0)", connection))
            await next.ExecuteNonQueryAsync();
        await using var read = new SqlCommand($"SELECT id, seq FROM {table} ORDER BY seq", connection);
        await using var reader = await read.ExecuteReaderAsync();
        foreach (var expected in new[] { 1, 2, 0 })
        {
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(((byte[])reader.GetValue(0))[15]).IsEqualTo((byte)expected);
        }
        await Assert.That(reader.GetInt64(1)).IsEqualTo(3L);
        await Assert.That(await reader.ReadAsync()).IsFalse();
    }
}
