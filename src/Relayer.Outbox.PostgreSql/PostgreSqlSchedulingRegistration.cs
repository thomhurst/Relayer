using Npgsql;

namespace Relayer.Outbox.PostgreSql;

// Created only by registrations that install an actual hosted due-row dispatcher.
internal sealed record PostgreSqlSchedulingRegistration(NpgsqlDataSource DataSource, string? Schema, string TablePrefix,
    int? OrderedLanes = null)
{
    private PostgreSqlDatabaseTarget Target
    {
        get;
    } = new(DataSource.ConnectionString);

    internal static (bool Unordered, bool Ordered) GetCapabilities(IEnumerable<PostgreSqlSchedulingRegistration> workers,
        string? schema, string prefix, int lanes, NpgsqlConnection? writer = null)
    {
        if (writer is not null && string.IsNullOrEmpty(writer.Database))
        {
            return default;
        }

        PostgreSqlDatabaseTarget? writerTarget = null;
        var unordered = false;
        var ordered = false;

        foreach (var worker in workers)
        {
            if ((worker.Schema ?? "public") != (schema ?? "public") || worker.TablePrefix != prefix ||
                (worker.OrderedLanes is { } workerLanes && workerLanes != lanes))
            {
                continue;
            }

            if (writer is not null && !worker.Target.MatchesWriter(worker.DataSource.ConnectionString, writer, ref writerTarget))
            {
                continue;
            }

            if (worker.OrderedLanes is null)
            {
                unordered = true;
            }
            else
            {
                ordered = true;
            }

            if (unordered && ordered)
            {
                break;
            }
        }

        return (unordered, ordered);
    }
}
