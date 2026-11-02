namespace Relayer.Outbox.PostgreSql;

// Created only by registrations that install an actual hosted due-row dispatcher.
internal sealed record PostgreSqlSchedulingRegistration(string? Schema, string TablePrefix, int? OrderedLanes = null)
{
    internal static bool Supports(IEnumerable<PostgreSqlSchedulingRegistration> workers, string? schema,
        string prefix, int? orderedLanes = null)
    {
        foreach (var worker in workers)
            if ((worker.Schema ?? "public") == (schema ?? "public") && worker.TablePrefix == prefix &&
                worker.OrderedLanes == orderedLanes) return true;
        return false;
    }
}
