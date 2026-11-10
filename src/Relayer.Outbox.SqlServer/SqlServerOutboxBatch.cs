using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;

namespace Relayer.Outbox.SqlServer;

// SqlBulkCopy consumes this reusable DbDataReader directly. No DataTable, parameter-per-row SQL or private batching engine.
internal sealed class SqlServerOutboxBatch : DbDataReader
{
    private SqlBulkCopy? _copy;
    private List<SqlServerBufferedMessage>? _rows;
    private int _count;
    private int _index;
    private SqlCommand? _reserve;
    private long _firstSequence;
    private readonly object _sequence = 0L;
    private readonly string[] _columns = ["id", "lane", "ordering_key", "transport", "destination", "message_type", "headers", "body", "created_at", "deliver_at", "tenant_id"];
    private readonly PayloadTextReader _text;
    private readonly PayloadStream _stream;

    internal SqlServerOutboxBatch()
    {
        var reads = new CompletedReads();
        _text = new(reads);
        _stream = new(reads);
    }

    internal void Prepare(SqlTransaction transaction, string table, List<SqlServerBufferedMessage> rows, int count, string? sequence = null)
    {
        _copy = new(transaction.Connection!, SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers, transaction)
        {
            DestinationTableName = table,
            EnableStreaming = true
        };
        // Explicit mappings omit server-owned defaults (attempts, status and fencing).
        for (var i = 0; i < _columns.Length; i++) _copy.ColumnMappings.Add(i, _columns[i]);
        // sp_sequence_get_range is the native range allocator. Assign its values by the reader's
        // send position, independent of the query planner or bulk-copy insertion order.
        if (sequence is not null)
        {
            _copy.ColumnMappings.Add(11, "seq");
            _reserve = new SqlCommand("sys.sp_sequence_get_range", transaction.Connection, transaction) { CommandType = CommandType.StoredProcedure };
            _reserve.Parameters.Add("@sequence_name", SqlDbType.NVarChar, 776).Value = sequence;
            _reserve.Parameters.Add("@range_size", SqlDbType.BigInt).Value = (long)count;
            _reserve.Parameters.Add("@range_first_value", SqlDbType.Variant).Direction = ParameterDirection.Output;
        }
        _rows = rows;
        _count = count;
        _index = -1;
    }

    internal void Execute()
    {
        try
        {
            if (_reserve is not null) { _reserve.ExecuteNonQuery(); _firstSequence = (long)_reserve.Parameters["@range_first_value"].Value; }
            _copy!.WriteToServer(this);
        }
        finally { Clear(); }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_reserve is not null)
            {
                await _reserve.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                _firstSequence = (long)_reserve.Parameters["@range_first_value"].Value;
            }
            await _copy!.WriteToServerAsync(this, cancellationToken).ConfigureAwait(false);
        }
        finally { Clear(); }
    }

    private void Clear()
    {
        _reserve?.Dispose();
        _reserve = null;
        _copy?.Close();
        _copy = null;
        _text.Reset(default);
        _stream.Reset(default);
        _rows = null;
        _count = 0;
        _index = -1;
    }

    public override bool Read() => ++_index < _count;
    public override object GetValue(int i)
    {
        if (i != 11) return _rows![_index].GetValue(i);
        Unsafe.Unbox<long>(_sequence) = checked(_firstSequence + _index);
        return _sequence;
    }
    public override bool IsDBNull(int i) => GetValue(i) == DBNull.Value;
    public override int FieldCount => _reserve is null ? 11 : 12;
    public override bool IsClosed => false;
    public override bool HasRows => _count != 0;
    public override IEnumerator GetEnumerator() => throw new NotSupportedException();
    public override int Depth => 0;
    public override int RecordsAffected => -1;
    public override bool NextResult() => false;
    public override void Close() { }
    protected override void Dispose(bool disposing) { if (disposing) Clear(); base.Dispose(disposing); }
    public override Stream GetStream(int ordinal)
    {
        var stream = (MemoryStream)GetValue(ordinal);
        _stream.Reset(stream.GetBuffer().AsMemory(0, checked((int)stream.Length)));
        return _stream;
    }
    public override TextReader GetTextReader(int ordinal)
    {
        var value = GetValue(ordinal);
        _text.Reset(value is SqlChars chars ? chars.Buffer.AsMemory(0, checked((int)chars.Length)) : ((string)value).AsMemory());
        return _text;
    }
    // Native bulk copy uses ordinal mappings, scalar GetValue and sequential streams for max-length columns.
    public override DataTable? GetSchemaTable() => throw new NotSupportedException();
    public override string GetName(int i) => throw new NotSupportedException();
    public override int GetOrdinal(string name) => throw new NotSupportedException();
    [return: DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type GetFieldType(int i) => throw new NotSupportedException();
    public override string GetDataTypeName(int i) => throw new NotSupportedException();
    public override int GetValues(object[] values) => throw new NotSupportedException();
    public override object this[int i] => GetValue(i);
    public override object this[string name] => throw new NotSupportedException();
    public override bool GetBoolean(int i) => throw new NotSupportedException();
    public override byte GetByte(int i) => throw new NotSupportedException();
    public override long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
    public override char GetChar(int i) => throw new NotSupportedException();
    public override long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
    public override Guid GetGuid(int i) => throw new NotSupportedException();
    public override short GetInt16(int i) => throw new NotSupportedException();
    public override int GetInt32(int i) => throw new NotSupportedException();
    public override long GetInt64(int i) => throw new NotSupportedException();
    public override float GetFloat(int i) => throw new NotSupportedException();
    public override double GetDouble(int i) => throw new NotSupportedException();
    public override string GetString(int i) => throw new NotSupportedException();
    public override decimal GetDecimal(int i) => throw new NotSupportedException();
    public override DateTime GetDateTime(int i) => throw new NotSupportedException();

    // SqlClient 7.1.1 streams in 4096-byte/character chunks through Task-returning methods.
    // Cache completed results after warmup instead of allocating a Task per column/chunk.
    private sealed class CompletedReads
    {
        private Task<int>?[] _results = new Task<int>?[4097];
        internal Task<int> Get(int count)
        {
            if (count >= _results.Length) Array.Resize(ref _results, count + 1);
            return _results[count] ??= Task.FromResult(count);
        }
    }

    private sealed class PayloadStream(CompletedReads reads) : Stream
    {
        private ReadOnlyMemory<byte> _memory;
        private int _position;
        internal void Reset(ReadOnlyMemory<byte> memory) { _memory = memory; _position = 0; }
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
        public override int Read(Span<byte> buffer)
        {
            var count = Math.Min(buffer.Length, _memory.Length - _position);
            _memory.Span.Slice(_position, count).CopyTo(buffer);
            _position += count;
            return count;
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return reads.Get(Read(buffer.AsSpan(offset, count)));
        }
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new(Read(buffer.Span));
        }
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _memory.Length;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class PayloadTextReader(CompletedReads reads) : TextReader
    {
        private ReadOnlyMemory<char> _memory;
        private int _position;
        internal void Reset(ReadOnlyMemory<char> memory) { _memory = memory; _position = 0; }
        public override int Peek() => _position < _memory.Length ? _memory.Span[_position] : -1;
        public override int Read() => _position < _memory.Length ? _memory.Span[_position++] : -1;
        public override int Read(char[] buffer, int index, int count) => Read(buffer.AsSpan(index, count));
        public override int Read(Span<char> buffer)
        {
            var count = Math.Min(buffer.Length, _memory.Length - _position);
            _memory.Span.Slice(_position, count).CopyTo(buffer);
            _position += count;
            return count;
        }
        public override Task<int> ReadAsync(char[] buffer, int index, int count)
        {
            return reads.Get(Read(buffer.AsSpan(index, count)));
        }
        public override Task<int> ReadBlockAsync(char[] buffer, int index, int count) => ReadAsync(buffer, index, count);
        public override int ReadBlock(char[] buffer, int index, int count) => Read(buffer, index, count);
        public override ValueTask<int> ReadAsync(Memory<char> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new(Read(buffer.Span));
        }
    }
}
