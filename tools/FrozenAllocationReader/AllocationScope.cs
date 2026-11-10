namespace Relayer.Diagnostics;

internal static class AllocationScope
{
    public static bool Includes(bool wholeProcess, int process, double time, int measurementProcess, double start, double stop)
        => wholeProcess || (process == measurementProcess && time > start && time < stop);
}
