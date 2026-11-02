using Microsoft.Extensions.Options;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

// Share immutable defaults across delivery scopes without building SQL or retaining scoped services.
internal sealed class DefaultOutboxCapabilities(IOptionsMonitor<EntityFrameworkCoreOutboxOptions> options,
    IEnumerable<PostgreSqlSchedulingRegistration> workers)
{
    private Snapshot? _current;

    internal Snapshot Get()
    {
        var value = options.CurrentValue;
        var current = Volatile.Read(ref _current);
        if (current is not null && ReferenceEquals(current.Options, value)) return current;

        current = new(value,
            PostgreSqlSchedulingRegistration.Supports(workers, value.Schema, value.TablePrefix),
            PostgreSqlSchedulingRegistration.Supports(workers, value.Schema, value.TablePrefix, value.Lanes));
        // Concurrent reloads may duplicate this cold work; each scope still receives one coherent version.
        Volatile.Write(ref _current, current);
        return current;
    }

    internal sealed record Snapshot(EntityFrameworkCoreOutboxOptions Options, bool Scheduling, bool OrderedScheduling);
}
