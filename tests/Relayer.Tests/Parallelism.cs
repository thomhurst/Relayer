using Relayer.Tests;
using TUnit.Core.Interfaces;

// Bound this process's combined broker load, including emulator SQL dependencies,
// across classes. Hold permits through fixture teardown, not only container startup.
// Other test processes and workers sharing Docker have independent limits.
[assembly: ParallelLimiter<CoreTestParallelLimit>]

namespace Relayer.Tests;

public sealed class CoreTestParallelLimit : IParallelLimit
{
    public int Limit => 2;
}
