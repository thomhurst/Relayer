using System.Data.Common;
using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;

namespace Relayer.Outbox.PostgreSql;

// Column storage is reused across commits. Parameter count is fixed, including large transactions.
internal sealed class PostgreSqlOutboxBatch : IOutboxBatch
{
    internal static string CreateInsertSql(string table) => $"""
        INSERT INTO {table}
            (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at, tenant_id)
        SELECT id, lane, convert_from(key, 'UTF8'), transport, destination, type, headers, body, created, delivery, tenant
        FROM unnest(@ids, @lanes, @keys, @transports, @destinations, @types, @headers, @bodies, @created, @delivery, @tenants)
            WITH ORDINALITY AS rows(id, lane, key, transport, destination, type, headers, body, created, delivery, tenant, position)
        ORDER BY position
        """;

    private readonly List<Guid> _ids = [];
    private readonly List<int?> _lanes = [];
    private readonly List<ReadOnlyMemory<byte>?> _keys = [];
    private readonly List<string> _transports = [];
    private readonly List<string> _destinations = [];
    private readonly List<string> _types = [];
    private readonly List<ReadOnlyMemory<byte>> _headers = [];
    private readonly List<ReadOnlyMemory<byte>> _bodies = [];
    private readonly List<DateTimeOffset> _created = [];
    private readonly List<DateTimeOffset?> _delivery = [];
    private readonly List<string?> _tenants = [];
    private readonly NpgsqlCommand _command = new();

    internal PostgreSqlOutboxBatch()
    {
        _command.Parameters.AddWithValue("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, _ids);
        _command.Parameters.AddWithValue("lanes", NpgsqlDbType.Array | NpgsqlDbType.Integer, _lanes);
        _command.Parameters.AddWithValue("keys", NpgsqlDbType.Array | NpgsqlDbType.Bytea, _keys);
        _command.Parameters.AddWithValue("transports", NpgsqlDbType.Array | NpgsqlDbType.Text, _transports);
        _command.Parameters.AddWithValue("destinations", NpgsqlDbType.Array | NpgsqlDbType.Text, _destinations);
        _command.Parameters.AddWithValue("types", NpgsqlDbType.Array | NpgsqlDbType.Text, _types);
        _command.Parameters.AddWithValue("headers", NpgsqlDbType.Array | NpgsqlDbType.Bytea, _headers);
        _command.Parameters.AddWithValue("bodies", NpgsqlDbType.Array | NpgsqlDbType.Bytea, _bodies);
        _command.Parameters.AddWithValue("created", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz, _created);
        _command.Parameters.AddWithValue("delivery", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz, _delivery);
        _command.Parameters.AddWithValue("tenants", NpgsqlDbType.Array | NpgsqlDbType.Text, _tenants);
    }

    public void Prepare(DbTransaction transaction, string insertSql)
    {
        Clear();
        _command.Connection = (NpgsqlConnection)transaction.Connection!;
        _command.Transaction = (NpgsqlTransaction)transaction;
        _command.CommandText = insertSql;
    }

    public void AddRange(ReadOnlySpan<BufferedOutboxMessage> rows)
    {
        foreach (var row in rows) Add(row);
    }

    internal void Add(BufferedOutboxMessage row)
    {
        _ids.Add(row.Id);
        _lanes.Add(row.Lane);
        _keys.Add(row.OrderingKey);
        _transports.Add(row.Transport);
        _destinations.Add(row.Destination);
        _types.Add(row.MessageType);
        _headers.Add(row.Headers);
        _bodies.Add(row.Body);
        _created.Add(row.CreatedAt);
        _delivery.Add(row.DeliverAt);
        _tenants.Add(row.TenantId);
    }

    public void Execute()
    {
        try { _command.ExecuteNonQuery(); }
        finally { Clear(); }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        try { await _command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false); }
        finally { Clear(); }
    }

    private void Clear()
    {
        _ids.Clear();
        _lanes.Clear();
        _keys.Clear();
        _transports.Clear();
        _destinations.Clear();
        _types.Clear();
        _headers.Clear();
        _bodies.Clear();
        _created.Clear();
        _delivery.Clear();
        _tenants.Clear();
        _command.Transaction = null;
        _command.Connection = null;
    }

    public void Dispose() => _command.Dispose();
}
