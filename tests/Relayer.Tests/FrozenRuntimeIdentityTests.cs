using Relayer.Diagnostics;

namespace Relayer.Tests;

public class FrozenRuntimeIdentityTests
{
    [Test]
    [Arguments("Microsoft-Windows-DotNETRuntime", "Runtime/Start", true)]
    [Arguments("Microsoft-Windows-DotNETRuntimeRundown", "Runtime/Start", true)]
    [Arguments("Microsoft-Windows-DotNETRuntimeRundown", "Loader/ModuleDCStop", true)]
    [Arguments("Microsoft-Windows-DotNETRuntimeRundown", "Loader/DomainModuleDCStop", true)]
    [Arguments("Microsoft-Windows-DotNETRuntime", "Loader/ModuleLoad", true)]
    [Arguments("Microsoft-Windows-DotNETRuntime", "GC/AllocationTick", false)]
    [Arguments("Microsoft-Windows-DotNETRuntime", "Method/LoadVerbose", false)]
    [Arguments("Other", "Runtime/Start", false)]
    [Arguments("Microsoft-Windows-DotNETRuntimePrivate", "Loader/ModuleLoad", false)]
    public async Task RetainsRuntimeAndModuleRecordsWithoutUnrelatedEvents(string provider, string eventName, bool expected)
    {
        await Assert.That(RuntimeIdentityScope.Includes(provider, eventName)).IsEqualTo(expected);
    }
}
