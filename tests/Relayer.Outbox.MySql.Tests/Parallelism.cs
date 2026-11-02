using Relayer.Outbox.MySql.Tests;
using TUnit.Core.Interfaces;

[assembly: ParallelLimiter<MySqlTestParallelLimit>]

namespace Relayer.Outbox.MySql.Tests;

public sealed class MySqlTestParallelLimit : IParallelLimit
{
    public int Limit => 2;
}
