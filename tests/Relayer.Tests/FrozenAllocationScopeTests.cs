using Relayer.Diagnostics;

namespace Relayer.Tests;

public class FrozenAllocationScopeTests
{
    [Test]
    public async Task WholeProcessIncludesTicksWithoutMarkersAndAcrossProcesses()
    {
        await Assert.That(AllocationScope.Includes(true, 7, 0, 0, 0, 0)).IsTrue();
        await Assert.That(AllocationScope.Includes(true, 8, 200, 7, 10, 20)).IsTrue();
    }

    [Test]
    public async Task MeasurementPreservesStrictBoundariesAndProcessIsolation()
    {
        await Assert.That(AllocationScope.Includes(false, 7, 15, 7, 10, 20)).IsTrue();
        await Assert.That(AllocationScope.Includes(false, 8, 15, 7, 10, 20)).IsFalse();
        await Assert.That(AllocationScope.Includes(false, 7, 10, 7, 10, 20)).IsFalse();
        await Assert.That(AllocationScope.Includes(false, 7, 20, 7, 10, 20)).IsFalse();
        await Assert.That(AllocationScope.Includes(false, 7, 21, 7, 10, 20)).IsFalse();
    }
}
