using System.Data.Common;

namespace Relayer;

// Shared native batch seam for EF coordination; providers retain their own reusable storage and commands.
internal interface IOutboxBatch : IDisposable
{
    void Prepare(DbTransaction transaction, string insertSql);
    void AddRange(ReadOnlySpan<BufferedOutboxMessage> rows);
    void Execute();
    ValueTask ExecuteAsync(CancellationToken cancellationToken);
}
