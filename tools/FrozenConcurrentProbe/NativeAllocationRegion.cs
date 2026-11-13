using System.Runtime.InteropServices;

// External diagnostic only. Loading/metadata work happens outside the measured region.
internal sealed class NativeAllocationRegion
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int Transition();
    private readonly Transition begin;
    private readonly Transition end;

    private NativeAllocationRegion(string path)
    {
        var library = NativeLibrary.Load(Path.GetFullPath(path));
        begin = Marshal.GetDelegateForFunctionPointer<Transition>(NativeLibrary.GetExport(library, "BeginAllocationRegion"));
        end = Marshal.GetDelegateForFunctionPointer<Transition>(NativeLibrary.GetExport(library, "EndAllocationRegion"));
    }

    internal static NativeAllocationRegion? Load() =>
        Environment.GetEnvironmentVariable("RELAYER_ALLOCATION_PROFILER") is { Length: > 0 } path ? new(path) : null;

    internal void Begin()
    {
        if (begin() != 1) throw new InvalidOperationException("Native allocation profiler is absent, uninitialized, or the region already began.");
    }

    internal void End()
    {
        if (end() != 1) throw new InvalidOperationException("Native allocation report failed; inspect its coverage record before interpreting counts.");
    }
}
