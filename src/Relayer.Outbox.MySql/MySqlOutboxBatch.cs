using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

// Reuse native batching and parameter storage; MySqlConnector owns packet sizing and execution.
internal sealed class MySqlOutboxBatch(string insertSql) : IDisposable
{
    private readonly MySqlBatch _batch = new();
    private readonly List<Row> _rows = [];
    private int _count;

    internal static string CreateInsertSql(string table) => $"""
        INSERT INTO {table}
            (id, lane, ordering_key, transport, destination, message_type, headers, body, created_at, deliver_at, tenant_id)
        VALUES (@id, CAST(@lane AS SIGNED), CONVERT(@key USING utf8mb4), @transport, @destination, @type,
            @headers, @body, @created, @delivery, @tenant)
        """;

    internal void Prepare(MySqlTransaction transaction)
    {
        _batch.Connection = transaction.Connection;
        _batch.Transaction = transaction;
    }

    internal void Add(BufferedOutboxMessage message)
    {
        if (_count == _rows.Count) _rows.Add(new(insertSql));
        var row = _rows[_count++];
        row.Set(message);
        _batch.BatchCommands.Add(row.Command);
    }

    internal void Execute()
    {
        try { _batch.ExecuteNonQuery(); }
        finally { Clear(); }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        try { await _batch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false); }
        finally { Clear(); }
    }

    private void Clear()
    {
        for (var i = 0; i < _count; i++) _rows[i].Clear();
        _count = 0;
        _batch.BatchCommands.Clear();
        _batch.Transaction = null;
        _batch.Connection = null;
    }

    public void Dispose() { Clear(); _batch.Dispose(); }

    internal sealed class Row
    {
        private readonly byte[] _id = new byte[16];
        private readonly char[] _created = new char[26];
        private readonly char[] _delivery = new char[26];
        private readonly char[] _lane = new char[10];
        private readonly object _deliveryView;
        private readonly object _laneView;
        private readonly MemoryParameter _body;
        private readonly MemoryParameter _headers;
        private readonly MemoryParameter _key;
        internal MySqlBatchCommand Command { get; }

        internal Row(string sql)
        {
            Command = new(sql);
            Add("id", MySqlDbType.Binary, _id);
            Add("created", MySqlDbType.VarChar, (ReadOnlyMemory<char>)_created);
            _deliveryView = (ReadOnlyMemory<char>)_delivery;
            _laneView = (ReadOnlyMemory<char>)_lane;
            Add("delivery", MySqlDbType.VarChar, DBNull.Value);
            Add("lane", MySqlDbType.VarChar, DBNull.Value);
            Add("transport", MySqlDbType.LongText, "");
            Add("destination", MySqlDbType.LongText, "");
            Add("type", MySqlDbType.LongText, "");
            Add("tenant", MySqlDbType.LongText, DBNull.Value);
            _body = new(Add("body", MySqlDbType.LongBlob, DBNull.Value));
            _headers = new(Add("headers", MySqlDbType.LongBlob, DBNull.Value));
            _key = new(Add("key", MySqlDbType.Blob, DBNull.Value));
        }

        private MySqlParameter Add(string name, MySqlDbType type, object value)
        {
            var parameter = new MySqlParameter(name, type) { Value = value };
            Command.Parameters.Add(parameter);
            return parameter;
        }

        internal void Set(BufferedOutboxMessage row)
        {
            row.Id.TryWriteBytes(_id, bigEndian: true, out _);
            Timestamp(row.CreatedAt, _created);
            if (row.DeliverAt is { } delivery) Timestamp(delivery, _delivery);
            Command.Parameters["delivery"].Value = row.DeliverAt.HasValue ? _deliveryView : DBNull.Value;
            if (row.Lane is { } lane) lane.TryFormat(_lane, out _, "D10", CultureInfo.InvariantCulture);
            Command.Parameters["lane"].Value = row.Lane.HasValue ? _laneView : DBNull.Value;
            Command.Parameters["transport"].Value = row.Transport;
            Command.Parameters["destination"].Value = row.Destination;
            Command.Parameters["type"].Value = row.MessageType;
            Command.Parameters["tenant"].Value = (object?)row.TenantId ?? DBNull.Value;
            _body.Set(row.Body);
            _headers.Set(row.Headers);
            _key.Set(row.OrderingKey);
        }

        internal void Clear()
        {
            Command.Parameters["transport"].Value = "";
            Command.Parameters["destination"].Value = "";
            Command.Parameters["type"].Value = "";
            Command.Parameters["tenant"].Value = DBNull.Value;
            _body.Release();
            _headers.Release();
            _key.Release();
        }

        private static void Timestamp(DateTimeOffset value, Span<char> buffer) =>
            value.UtcDateTime.TryFormat(buffer, out _, "yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

        // MySqlParameter serializes MemoryStream through TryGetBuffer. A reference-valued
        // slice avoids boxing ReadOnlyMemory<byte> when payload lengths change, without copying.
        private sealed class MemoryParameter(MySqlParameter parameter) : MemoryStream
        {
            private ReadOnlyMemory<byte> _memory;
            public override long Length => _memory.Length;
            public override bool TryGetBuffer(out ArraySegment<byte> buffer) => MemoryMarshal.TryGetArray(_memory, out buffer);
            public override byte[] ToArray() => _memory.ToArray();

            internal void Set(ReadOnlyMemory<byte>? memory)
            {
                _memory = memory.GetValueOrDefault();
                parameter.Value = memory.HasValue ? this : DBNull.Value;
            }
            internal void Release()
            {
                parameter.Value = DBNull.Value;
                _memory = default;
            }
        }
    }
}
