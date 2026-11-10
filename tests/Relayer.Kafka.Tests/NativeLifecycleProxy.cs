using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Relayer.Kafka.Tests;

// Test-only forwarding instrumentation. Every invocation still executes on the real Dekaf client.
// Facets and topic producers share the owning client's closure boundary.
internal sealed class NativeLifecycleProbe
{
    private readonly ConcurrentQueue<string> _violations = new();
    internal int Closed;
    internal int Commits;

    internal void Before(string operation)
    {
        if (Volatile.Read(ref Closed) != 0 && operation != nameof(IAsyncDisposable.DisposeAsync))
            _violations.Enqueue(operation);
    }
    internal void AssertClean()
    {
        if (!_violations.IsEmpty)
            throw new InvalidOperationException("Native Kafka calls after disposal: " + string.Join(", ", _violations));
    }
    internal void AssertClosed()
    {
        AssertClean();
        if (Volatile.Read(ref Closed) == 0) throw new InvalidOperationException("Native Kafka client was never disposed.");
    }
}

internal class NativeLifecycleProxy : DispatchProxy
{
    private object _native = null!;
    private NativeLifecycleProbe _probe = null!;

    internal static T Wrap<T>(T native, NativeLifecycleProbe probe) where T : class
    {
        var wrapped = Create<T, NativeLifecycleProxy>();
        var proxy = (NativeLifecycleProxy)(object)wrapped;
        proxy._native = native;
        proxy._probe = probe;
        return wrapped;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        var method = targetMethod!;
        _probe.Before(method.Name);
        if (method.Name == nameof(IAsyncDisposable.DisposeAsync)) Interlocked.Exchange(ref _probe.Closed, 1);
        object? result;
        try { result = method.Invoke(_native, args); }
        catch (TargetInvocationException error) when (error.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(error.InnerException).Throw();
            throw;
        }
        if (method.Name == "CommitAsync" && result is ValueTask commit) return ObserveCommitAsync(commit);
        // Dekaf's offsets, seek and topic-producer facets are interfaces too.
        if (result is not null && method.ReturnType.IsInterface && (method.ReturnType.Namespace?.StartsWith("Dekaf", StringComparison.Ordinal) == true || method.Name == "ForTopic"))
            return typeof(NativeLifecycleProxy).GetMethod(nameof(Wrap), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(method.ReturnType).Invoke(null, [result, _probe]);
        return result;
    }

    private async ValueTask ObserveCommitAsync(ValueTask commit)
    {
        await commit;
        Interlocked.Increment(ref _probe.Commits);
    }
}
