using Relayer.Outbox.PostgreSql.Tests;
using TUnit.Core.Interfaces;

// Each database test starts its own PostgreSQL container. Bound their combined
// startup and connection load, including across different test classes.
[assembly: ParallelLimiter<PostgreSqlTestParallelLimit>]

namespace Relayer.Outbox.PostgreSql.Tests;

public sealed class PostgreSqlTestParallelLimit : IParallelLimit
{
    public int Limit => 4;
}
