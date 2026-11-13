using Relayer.Transports;
using System.Globalization;
using System.Runtime.CompilerServices;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

// Native MySqlBatch owns packet construction and cancellation. ROW_COUNT() supplies each
// conditional mutation's outcome because MySqlBatchCommand.RecordsAffected is always zero.
internal sealed class MySqlDispatchMutations(string sql) : IDisposable
{
    private readonly MySqlBatch _batch = new();
    private readonly List<Row> _rows = [];

    internal void Prepare(ReadOnlySpan<OutboxClaim> claims, DateTimeOffset now, DateTimeOffset expires)
    {
        _batch.BatchCommands.Clear();
        for (var i = 0; i < claims.Length; i++)
        {
            if (i == _rows.Count) _rows.Add(new(sql));
            var row = _rows[i];
            row.Set(claims[i], now, expires);
            _batch.BatchCommands.Add(row.Command);
        }
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<long> ExecuteAsync(MySqlConnection connection, Memory<bool> outcomes,
        CancellationToken cancellationToken)
    {
        _batch.Connection = connection;
        try
        {
            await using var reader = await _batch.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            long total = 0;
            for (var i = 0; i < _batch.BatchCommands.Count; i++)
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    throw new InvalidOperationException("MySQL dispatch mutation did not return its affected-row count.");
                var count = reader.GetInt64(0);
                total += count;
                if (!outcomes.IsEmpty) outcomes.Span[i] = count == 1;
                if (i + 1 < _batch.BatchCommands.Count && !await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    throw new InvalidOperationException("MySQL dispatch mutation batch ended before all outcomes were returned.");
            }
            return total;
        }
        finally { _batch.Connection = null; _batch.BatchCommands.Clear(); }
    }

    public void Dispose() => _batch.Dispose();

    private sealed class Row
    {
        private readonly byte[] _id = new byte[16];
        private readonly byte[] _token = new byte[16];
        private readonly char[] _now = new char[26];
        private readonly char[] _expires = new char[26];
        internal MySqlBatchCommand Command { get; }

        internal Row(string sql)
        {
            Command = new(sql + "; SELECT ROW_COUNT();");
            Command.Parameters.Add(new("id", MySqlDbType.Binary) { Value = _id });
            Command.Parameters.Add(new("token", MySqlDbType.Binary) { Value = _token });
            Command.Parameters.Add(new("now", MySqlDbType.VarChar) { Value = (ReadOnlyMemory<char>)_now });
            Command.Parameters.Add(new("expires", MySqlDbType.VarChar) { Value = (ReadOnlyMemory<char>)_expires });
        }

        internal void Set(OutboxClaim claim, DateTimeOffset now, DateTimeOffset expires)
        {
            claim.MessageId.TryWriteBytes(_id, bigEndian: true, out _);
            claim.Token.TryWriteBytes(_token, bigEndian: true, out _);
            now.UtcDateTime.TryFormat(_now, out _, "yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
            expires.UtcDateTime.TryFormat(_expires, out _, "yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
        }
    }
}
