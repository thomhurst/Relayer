namespace Relayer.Outbox.PostgreSql;

// Ownership moves from the committing scope to the worker. Swap reusable buffers, never borrow a DbContext.
internal sealed class OutboxDispatchBatch
{
    internal List<BufferedOutboxMessage> Rows { get; } = [];
    internal int Count { get; private set; }

    internal void Take(List<BufferedOutboxMessage> source, int count, DateTimeOffset now)
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

    internal void Sort() => Rows.Sort(0, Count, RowComparer.Instance);

    internal void Clear()
    {
        for (var i = 0; i < Count; i++) Rows[i].Clear();
        Count = 0;
    }

    private sealed class RowComparer : IComparer<BufferedOutboxMessage>
    {
        internal static readonly RowComparer Instance = new();
        public int Compare(BufferedOutboxMessage? x, BufferedOutboxMessage? y)
        {
            var transport = StringComparer.Ordinal.Compare(x!.Transport, y!.Transport);
            return transport != 0 ? transport : StringComparer.Ordinal.Compare(x.Destination, y.Destination);
        }
    }
}
