using Microsoft.Extensions.DependencyInjection;

namespace Relayer;

// Resolve scoped executors once during composition, never retain a scoped service in the pipeline.
internal sealed class OutboxCapabilities(IServiceScopeFactory scopes)
{
    private readonly Lazy<Snapshot> _snapshot = new(() => Resolve(scopes));
    internal Snapshot Get() => _snapshot.Value;

    private static Snapshot Resolve(IServiceScopeFactory scopes)
    {
        using var scope = scopes.CreateScope();
        var executor = scope.ServiceProvider.GetService<IOutboxEnlistment>();
        return new(executor is not null, executor?.SupportsScheduling == true,
            executor?.SupportsOrderedScheduling == true);
    }

    internal sealed record Snapshot(bool Transactions, bool Scheduling, bool OrderedScheduling);
}
