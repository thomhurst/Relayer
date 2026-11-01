using System.Runtime.CompilerServices;

namespace Relayer.Tests;

public static class TestConfiguration
{
    [ModuleInitializer]
    public static void DisableReportListener()
    {
        // The HTML reporter listens to every ActivitySource and overrides sampling decisions.
        // Keep this setting in a dedicated executable so other suites retain their reports.
        Environment.SetEnvironmentVariable("TUNIT_DISABLE_HTML_REPORTER", "true");
    }
}
