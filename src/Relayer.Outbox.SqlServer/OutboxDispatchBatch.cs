using System.Runtime.InteropServices;

namespace Relayer.Outbox.SqlServer;

// Ownership moves from the committing scope to the worker. Swap reusable buffers, never borrow a DbContext.
internal sealed class OutboxDispatchBatch
{
    private static readonly Comparison<SqlServerBufferedMessage> Comparison = Compare;
    internal List<SqlServerBufferedMessage> Rows { get; } = [];
    internal int Count { get; private set; }

    internal void Take(List<SqlServerBufferedMessage> source, int count, DateTimeOffset now)
    {
        for (var i = 0; i < count; i++)
        {
            var row = source[i];
            if (row.Lane is not null || row.DeliverAt > now) continue;
            if (Count == Rows.Count) Rows.Add(new());
            source[i] = Rows[Count];
            Rows[Count++] = row;
        }
    }

    // Take excludes ordered lanes, so unstable sorting cannot change an ordering guarantee.
    internal void Sort() => CollectionsMarshal.AsSpan(Rows)[..Count].Sort(Comparison);

    internal void Clear()
    {
        for (var i = 0; i < Count; i++) Rows[i].Clear();
        Count = 0;
    }

    private static int Compare(SqlServerBufferedMessage x, SqlServerBufferedMessage y)
    {
        var transport = StringComparer.Ordinal.Compare(x.Transport, y.Transport);
        return transport != 0 ? transport : StringComparer.Ordinal.Compare(x.Destination, y.Destination);
    }
}
