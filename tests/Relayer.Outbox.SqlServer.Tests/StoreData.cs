using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer.Tests;

internal sealed class StoreData(SqlServerFixture database, SqlServerOutboxOptions? options = null)
{
    internal SqlServerOutboxOptions Options { get; } = options ?? new() { TablePrefix = "t_" + Guid.NewGuid().ToString("N") + "_" };
    internal string Table => "[" + (Options.Schema ?? "dbo").Replace("]", "]]") + "].[" + Options.TablePrefix.Replace("]", "]]") + "outbox]";
    internal SqlServerOutboxStore Store() => new(database.ConnectionString, Options);

    internal async Task Create()
    {
        await Execute(SqlServerOutboxSchema.CreateScript(Options));
    }

    internal async Task Execute(string sql)
    {
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    internal async Task Insert(Guid id, DateTimeOffset now, int? lane = null, DateTimeOffset? deliverAt = null,
        string status = "pending", DateTimeOffset? expires = null, int bodySize = 3)
    {
        // Independent encoding fixture: little-endian version/count and byte lengths.
        using var headers = new MemoryStream();
        using (var writer = new BinaryWriter(headers, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(1);
            writer.Write(3);
            WriteHeader(writer, "duplicate", Encoding.UTF8.GetBytes("é"));
            WriteHeader(writer, "duplicate", [255, 0, 254]);
            WriteHeader(writer, "empty", []);
        }
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"""
            INSERT INTO {Table} (id, lane, transport, message_type, destination, body, headers, ordering_key,
                deliver_at, created_at, status, claim_token, claim_expires_at, tenant_id)
            VALUES (@id, @lane, N'events', N'order.placed.v1', N'orders', @body, @headers, N'customer-é',
                @due, @now, @status, @token, @expires, N'tenant-é')
            """, connection);
        command.Parameters.Add("@id", SqlDbType.Binary, 16).Value = id.ToByteArray(bigEndian: true);
        command.Parameters.Add("@lane", SqlDbType.Int).Value = (object?)lane ?? DBNull.Value;
        var body = new byte[bodySize];
        if (bodySize >= 3) { body[0] = 0; body[1] = 255; body[2] = 1; }
        command.Parameters.Add("@body", SqlDbType.VarBinary, -1).Value = body;
        command.Parameters.Add("@headers", SqlDbType.VarBinary, -1).Value = headers.ToArray();
        command.Parameters.Add("@due", SqlDbType.DateTimeOffset).Value = (object?)deliverAt ?? DBNull.Value;
        command.Parameters.Add("@now", SqlDbType.DateTimeOffset).Value = now;
        command.Parameters.Add("@status", SqlDbType.VarChar, 7).Value = status;
        command.Parameters.Add("@token", SqlDbType.Binary, 16).Value = expires.HasValue ? Guid.NewGuid().ToByteArray(bigEndian: true) : DBNull.Value;
        command.Parameters.Add("@expires", SqlDbType.DateTimeOffset).Value = (object?)expires ?? DBNull.Value;
        await command.ExecuteNonQueryAsync();
    }

    private static void WriteHeader(BinaryWriter writer, string name, byte[] value)
    {
        var bytes = Encoding.UTF8.GetBytes(name);
        writer.Write(bytes.Length);
        writer.Write(bytes);
        writer.Write(value.Length);
        writer.Write(value);
    }
}
