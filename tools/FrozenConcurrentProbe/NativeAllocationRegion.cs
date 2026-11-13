using System.Runtime.InteropServices;

// External diagnostic only. Loading/metadata work happens outside the measured region.
internal sealed class NativeAllocationRegion
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int Transition();
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int CheckpointTransition(int index, long measured, int final);
    private readonly CheckpointTransition? checkpoint;
    private readonly Transition begin;
    private readonly Transition end;

    private NativeAllocationRegion(string path, bool checkpoints)
    {
        var library = NativeLibrary.Load(Path.GetFullPath(path));
        begin = Marshal.GetDelegateForFunctionPointer<Transition>(NativeLibrary.GetExport(library, "BeginAllocationRegion"));
        if (checkpoints)
            checkpoint = Marshal.GetDelegateForFunctionPointer<CheckpointTransition>(NativeLibrary.GetExport(library, "CheckpointAllocationRegion"));
        end = Marshal.GetDelegateForFunctionPointer<Transition>(NativeLibrary.GetExport(library, "EndAllocationRegion"));
    }

    internal static NativeAllocationRegion? Load(bool checkpoints = false) =>
        Environment.GetEnvironmentVariable("RELAYER_ALLOCATION_PROFILER") is { Length: > 0 } path ? new(path, checkpoints) : null;

    internal void Begin()
    {
        if (begin() != 1) throw new InvalidOperationException("Native allocation profiler is absent, uninitialized, or the region already began.");
    }

    internal void Checkpoint(int index, long measured, bool final = false)
    {
        if (checkpoint is null || checkpoint(index, measured, final ? 1 : 0) != 1)
            throw new InvalidOperationException("Native allocation checkpoint failed; retain the incomplete report.");
    }

    internal void End()
    {
        if (end() != 1) throw new InvalidOperationException("Native allocation report failed; inspect its coverage record before interpreting counts.");
    }
}
