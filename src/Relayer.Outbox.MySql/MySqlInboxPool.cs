using Relayer.Transports;
using System.Collections.Concurrent;
using MySqlConnector;

namespace Relayer.Outbox.MySql;

// Provider-owned, so independently disposed delivery scopes share scratch state. Separate pools
// preserve the table prefix of each options snapshot, including scopes overlapping a reload.
internal sealed class MySqlInboxPools : IDisposable
{
    private readonly ConcurrentDictionary<string, MySqlInboxPool> _pools = new(StringComparer.Ordinal);

    internal MySqlInboxPool Get(string prefix) => _pools.GetOrAdd(prefix,
        static value => new(new MySqlOutboxOptions { TablePrefix = value }));

    public void Dispose()
    {
        foreach (var pool in _pools.Values) pool.Dispose();
    }
}

internal sealed class MySqlInboxPool(MySqlOutboxOptions options) : IDisposable
{
    private readonly Segment _items = new();
    // Validated inputs and exact affected counts make IGNORE independent of CLIENT_FOUND_ROWS.
    private readonly string _sql = $"INSERT IGNORE INTO {MySqlOutboxSchema.InboxTable(options)} (consumer, message_id) VALUES (@consumer, @id)";

    internal Scratch Rent()
    {
        var segment = _items;
        while (true)
        {
            for (var i = 0; i < segment.Items.Length; i++)
            {
                var item = Volatile.Read(ref segment.Items[i]);
                if (item is null)
                {
                    var added = new Scratch(_sql);
                    item = Interlocked.CompareExchange(ref segment.Items[i], added, null);
                    if (item is null) item = added;
                    else added.Command.Dispose();
                }
                if (Volatile.Read(ref item.InUse) == 0 &&
                    Interlocked.CompareExchange(ref item.InUse, 1, 0) == 0) return item;
            }
            // Same high-water-mark strategy as SendTracing: warm bursts reuse all observed concurrency.
            var next = Volatile.Read(ref segment.Next);
            if (next is null)
            {
                var added = new Segment();
                next = Interlocked.CompareExchange(ref segment.Next, added, null) ?? added;
            }
            segment = next;
        }
    }

    internal void Return(Scratch item) => Volatile.Write(ref item.InUse, 0);

    public void Dispose()
    {
        for (var segment = _items; segment is not null; segment = segment.Next)
            foreach (var item in segment.Items) item?.Command.Dispose();
    }

    private sealed class Segment
    {
        internal readonly Scratch?[] Items = new Scratch[64];
        internal Segment? Next;
    }

    internal sealed class Scratch
    {
        internal int InUse;
        internal byte[] Id { get; } = new byte[16];
        internal MySqlCommand Command { get; }

        internal Scratch(string sql)
        {
            Command = new(sql);
            Command.Parameters.Add(new("consumer", MySqlDbType.VarBinary));
            Command.Parameters.Add(new("id", MySqlDbType.Binary) { Value = Id });
        }
    }
}
