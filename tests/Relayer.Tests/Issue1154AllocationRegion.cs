using System.Runtime.InteropServices;

namespace Relayer.Tests;

// Temporary bridge to the existing native allocation profiler.
internal sealed class NativeAllocationRegion
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int Transition();

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int CheckpointTransition(int index, long measured, int final);

    private readonly Transition _begin;
    private readonly CheckpointTransition _checkpoint;
    private readonly Transition _end;

    private NativeAllocationRegion(string path)
    {
        var library = NativeLibrary.Load(Path.GetFullPath(path));
        _begin = Marshal.GetDelegateForFunctionPointer<Transition>(NativeLibrary.GetExport(library, "BeginAllocationRegion"));
        _checkpoint = Marshal.GetDelegateForFunctionPointer<CheckpointTransition>(NativeLibrary.GetExport(library, "CheckpointAllocationRegion"));
        _end = Marshal.GetDelegateForFunctionPointer<Transition>(NativeLibrary.GetExport(library, "EndAllocationRegion"));
    }

    internal static NativeAllocationRegion? Load()
    {
        var path = Environment.GetEnvironmentVariable("RELAYER_ALLOCATION_PROFILER");
        return string.IsNullOrEmpty(path) ? null : new(path);
    }

    internal void Begin()
    {
        if (_begin() != 1)
        {
            throw new InvalidOperationException("Native allocation profiler could not begin the capture.");
        }
    }

    internal void Checkpoint(int index, long measured, bool final = false)
    {
        if (_checkpoint(index, measured, final ? 1 : 0) != 1)
        {
            throw new InvalidOperationException("Native allocation profiler could not record a checkpoint.");
        }
    }

    internal void End()
    {
        if (_end() != 1)
        {
            throw new InvalidOperationException("Native allocation profiler could not write the report.");
        }
    }
}
