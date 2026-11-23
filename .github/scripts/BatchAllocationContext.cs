using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace Relayer.Tests;

// Read-only diagnostic for the exact Linux x64 CoreCLR used in the failing run.
// The ELF TLS symbol is at 0xa0. The counter's disassembly supplies the field offsets.
internal static partial class BatchAllocationContext
{
    private const string RuntimeHash = "C2A641051BB6C968FA7BEACAA396C6F3FF8C0E578F6BC375678464F060C8D9CC";

    internal static nint Find()
    {
        if (!OperatingSystem.IsLinux() || RuntimeInformation.ProcessArchitecture != Architecture.X64)
        {
            throw new PlatformNotSupportedException("The allocation context probe requires Linux x64.");
        }

        var runtimePath = Path.Combine(Path.GetDirectoryName(typeof(object).Assembly.Location)!, "libcoreclr.so");
        using var runtime = File.OpenRead(runtimePath);

        if (Convert.ToHexString(SHA256.HashData(runtime)) != RuntimeHash)
        {
            throw new InvalidOperationException("The runtime hash does not match the verified CoreCLR layout.");
        }

        nint context = 0;
        IterateProgramHeaders((info, size, _) =>
        {
            if (size < 64 || Marshal.PtrToStringUTF8(Marshal.ReadIntPtr(info, 8)) is not { } name ||
                Path.GetFileName(name) != "libcoreclr.so")
            {
                return 0;
            }

            var tls = Marshal.ReadIntPtr(info, 56);
            context = tls == 0 ? 0 : tls + 0xa0;
            return 1;
        }, 0);

        if (context == 0)
        {
            throw new InvalidOperationException("The measured thread has no CoreCLR TLS block.");
        }

        return context;
    }

    internal static Snapshot Read(nint context)
    {
        return new(Marshal.ReadInt64(context, 8), Marshal.ReadInt64(context, 16),
            Marshal.ReadInt64(context, 24), Marshal.ReadInt64(context, 32));
    }

    internal readonly record struct Snapshot(long Pointer, long Limit, long SmallObjectBytes, long OtherBytes)
    {
        internal long Remaining => Limit - Pointer;

        internal long Counter => SmallObjectBytes + OtherBytes - Remaining;
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int ProgramHeaderVisitor(nint info, nuint size, nint data);

    [DllImport("libc", EntryPoint = "dl_iterate_phdr")]
    private static extern int IterateProgramHeaders(ProgramHeaderVisitor visit, nint data);
}
