using Relayer.Outbox.SqlServer.Tests;
using TUnit.Core.Interfaces;

// Tests share one SQL Server instance, including native 7,000-row bulk copies.
// Bound concurrent database work to reduce CI contention within the native bulk-copy timeout.
[assembly: ParallelLimiter<SqlServerTestParallelLimit>]

namespace Relayer.Outbox.SqlServer.Tests;

public sealed class SqlServerTestParallelLimit : IParallelLimit
{
    public int Limit => 4;
}
