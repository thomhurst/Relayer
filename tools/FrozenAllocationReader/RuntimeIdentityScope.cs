namespace Relayer.Diagnostics;

internal static class RuntimeIdentityScope
{
    public static bool Includes(string provider, string eventName) =>
        (provider == "Microsoft-Windows-DotNETRuntime" || provider == "Microsoft-Windows-DotNETRuntimeRundown") &&
        (eventName.StartsWith("Runtime/", StringComparison.Ordinal) ||
         eventName.StartsWith("Loader/Module", StringComparison.Ordinal) ||
         eventName.StartsWith("Loader/DomainModule", StringComparison.Ordinal));
}
