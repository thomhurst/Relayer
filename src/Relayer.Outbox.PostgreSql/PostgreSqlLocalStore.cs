using Npgsql;

namespace Relayer.Outbox.PostgreSql;

internal sealed record PostgreSqlLocalStore(string Name, string DispatchStoreName, NpgsqlDataSource DataSource,
    PostgreSqlOutboxOptions Options)
{
    internal bool MatchesTable(string? schema, string prefix)
    {
        return (Options.Schema ?? "public") == (schema ?? "public") && Options.TablePrefix == prefix;
    }

    internal bool SharesStore(PostgreSqlDispatchRegistration worker)
    {
        return MatchesTable(worker.Options.Schema, worker.Options.TablePrefix) &&
            (ReferenceEquals(DataSource, worker.DataSource) || MatchesDatabase(worker.DataSource.ConnectionString));
    }

    internal void ValidateWorker(PostgreSqlDispatchRegistration worker)
    {
        // Workers can claim the same rows through separate pools, but local delivery requires the shared source.
        if (!ReferenceEquals(DataSource, worker.DataSource))
        {
            throw Conflict(worker, nameof(NpgsqlDataSource));
        }

        if (!MatchesTable(worker.Options.Schema, worker.Options.TablePrefix))
        {
            throw Conflict(worker, "Schema and TablePrefix");
        }

        if (Options.MaxDispatchAttempts != worker.Options.MaxDispatchAttempts)
        {
            throw Conflict(worker, nameof(Options.MaxDispatchAttempts));
        }

        if (Options.RetainSentMessages != worker.Options.RetainSentMessages)
        {
            throw Conflict(worker, nameof(Options.RetainSentMessages));
        }

        if (worker.Kind == PostgreSqlDispatchKind.Ordered && Options.Lanes != worker.Options.Lanes)
        {
            throw Conflict(worker, nameof(Options.Lanes));
        }
    }

    internal void ValidateWriterLanes(int lanes)
    {
        if (Options.Lanes != lanes)
        {
            throw new InvalidOperationException($"{Store} uses Lanes={Options.Lanes}, but the PostgreSQL outbox writer uses Lanes={lanes}.");
        }
    }

    internal bool HasWriterLaneMismatch(string? schema, string prefix, int lanes)
    {
        return Options.Lanes != lanes && MatchesTable(schema, prefix);
    }

    internal bool MatchesDatabase(string connectionString)
    {
        if (DataSource.ConnectionString == connectionString)
        {
            return true;
        }

        var local = new NpgsqlConnectionStringBuilder(DataSource.ConnectionString);
        var other = new NpgsqlConnectionStringBuilder(connectionString);
        return local.Host == other.Host && local.Port == other.Port &&
            (local.Database ?? local.Username) == (other.Database ?? other.Username);
    }

    internal static void ValidateWriterConnection<T>(PostgreSqlLocalStore[] stores, string? schema,
        string prefix, int lanes, T writer, Func<T, string> getConnectionString)
    {
        string? connectionString = null;

        foreach (var local in stores)
        {
            if (local.HasWriterLaneMismatch(schema, prefix, lanes) &&
                local.MatchesDatabase(connectionString ??= getConnectionString(writer)))
            {
                local.ValidateWriterLanes(lanes);
            }
        }
    }

    private string Store =>
        $"PostgreSQL local transport '{Name}' in dispatch store '{DispatchStoreName}' (schema '{Options.Schema ?? "public"}', table prefix '{Options.TablePrefix}')";

    private InvalidOperationException Conflict(PostgreSqlDispatchRegistration worker, string setting)
    {
        return new($"{Store} and {worker.Kind} worker '{worker.Name}' must use the same {setting}.");
    }
}
