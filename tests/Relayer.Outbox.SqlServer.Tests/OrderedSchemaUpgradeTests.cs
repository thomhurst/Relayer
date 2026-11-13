using System.Data;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer.Tests;

[ClassDataSource<SqlServerFixture>(Shared = SharedType.PerTestSession)]
public sealed class OrderedSchemaUpgradeTests(SqlServerFixture database)
{
    [Test]
    public async Task Initial_schema_indexes_parked_lane_sequences()
    {
        var data = new StoreData(database);
        await data.Create();
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await AssertParkedIndex(connection, data.Options);
    }

    [Test]
    public async Task Ordered_membership_preserves_existing_maintenance_lease_schema()
    {
        var data = new StoreData(database);
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        var owner = Guid.NewGuid();
        await using (var existing = new SqlCommand($"""
            CREATE TABLE {SqlServerOutboxSchema.LeaseTable(data.Options)} (
                name varchar(16) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
                owner uniqueidentifier NOT NULL, expires_at datetimeoffset(7) NOT NULL);
            INSERT INTO {SqlServerOutboxSchema.LeaseTable(data.Options)} VALUES ('sweeper', @owner, DATEADD(hour, 1, SYSUTCDATETIME()));
            """, connection))
        {
            existing.Parameters.AddWithValue("owner", owner);
            await existing.ExecuteNonQueryAsync();
        }
        await data.Create();
        var coordinator = new SqlServerLaneCoordinator(database.ConnectionString, new()
        { TablePrefix = data.Options.TablePrefix, Lanes = 1 });
        await coordinator.RebalanceAsync(default);
        await using (var transaction = (SqlTransaction)await connection.BeginTransactionAsync())
        {
            await Assert.That(await coordinator.AcquireAsync(connection, transaction, 0, default)).IsTrue();
            await transaction.CommitAsync();
        }
        await coordinator.ReleaseAsync(default);
        await using var read = new SqlCommand($"SELECT owner FROM {SqlServerOutboxSchema.LeaseTable(data.Options)} WHERE name = 'sweeper'", connection);
        await Assert.That((Guid)(await read.ExecuteScalarAsync())!).IsEqualTo(owner);
    }

    [Test]
    public async Task Explicit_upgrade_preserves_historical_UUID_order_and_continues_native_sequence()
    {
        // Exercise both identifier and SQL literal escaping through the dynamic migration batches.
        var options = new SqlServerOutboxOptions { TablePrefix = "m_]'" + TestContext.Current!.Isolation.GetIsolatedName("upgrade") + "_" };
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
        await AssertParkedIndex(connection, options);
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

    private static async Task AssertParkedIndex(SqlConnection connection, SqlServerOutboxOptions options)
    {
        await using var command = new SqlCommand("""
            SELECT c.name, i.filter_definition
            FROM sys.indexes AS i
            JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(@table) AND i.name = @index AND ic.key_ordinal > 0
            ORDER BY ic.key_ordinal
            """, connection);
        command.Parameters.AddWithValue("table", SqlServerOutboxSchema.Table(options));
        command.Parameters.AddWithValue("index", options.TablePrefix + "outbox_parked_idx");
        await using var reader = await command.ExecuteReaderAsync();
        foreach (var key in new[] { "lane", "seq" })
        {
            await Assert.That(await reader.ReadAsync()).IsTrue();
            await Assert.That(reader.GetString(0)).IsEqualTo(key);
            await Assert.That(reader.GetString(1)).IsEqualTo("([status]='parked')");
        }
        await Assert.That(await reader.ReadAsync()).IsFalse();
    }
}
