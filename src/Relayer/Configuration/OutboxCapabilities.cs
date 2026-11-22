using Microsoft.Extensions.DependencyInjection;
using Relayer.Transports;

namespace Relayer;

// Cache capability values without retaining a scoped service in the pipeline.
internal sealed class OutboxCapabilities(IServiceScopeFactory scopes)
{
    private readonly Lazy<Snapshot> _standalone = new(() => Resolve(scopes));
    private Snapshot? _snapshot;

    internal Snapshot Get()
    {
        return Volatile.Read(ref _snapshot) ?? Cache(_standalone.Value);
    }

    internal async ValueTask<Snapshot> GetAsync()
    {
        if (Volatile.Read(ref _snapshot) is { } cached)
        {
            return cached;
        }

        Snapshot snapshot;
        var scope = scopes.CreateAsyncScope();

        await using (scope.ConfigureAwait(false))
        {
            snapshot = Read(scope.ServiceProvider);
        }

        return Cache(snapshot);
    }

    private Snapshot Cache(Snapshot snapshot)
    {
        return Interlocked.CompareExchange(ref _snapshot, snapshot, null) ?? snapshot;
    }

    private static Snapshot Resolve(IServiceScopeFactory scopes)
    {
        using var scope = scopes.CreateScope();
        return Read(scope.ServiceProvider);
    }

    private static Snapshot Read(IServiceProvider services)
    {
        var executor = services.GetService<IOutboxEnlistment>();
        return new(executor is not null, executor?.SupportsScheduling == true,
            executor?.SupportsOrderedScheduling == true);
    }

    internal sealed record Snapshot(bool Transactions, bool Scheduling, bool OrderedScheduling);
}
