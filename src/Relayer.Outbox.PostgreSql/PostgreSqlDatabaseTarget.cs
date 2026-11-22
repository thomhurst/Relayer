using Npgsql;

namespace Relayer.Outbox.PostgreSql;

internal readonly record struct PostgreSqlDatabaseTarget(string? Host, int Port, string? Database)
{
    internal PostgreSqlDatabaseTarget(string connectionString, string? database = null)
        : this(new NpgsqlConnectionStringBuilder(connectionString), database)
    {
    }

    private PostgreSqlDatabaseTarget(NpgsqlConnectionStringBuilder settings, string? database)
        : this(settings.Host, settings.Port, database ?? settings.Database ?? settings.Username)
    {
    }

    internal bool MatchesWriter(string configuredConnectionString, NpgsqlConnection writer,
        ref PostgreSqlDatabaseTarget? writerTarget)
    {
        var database = writer.Database;
        return Database == database &&
            (configuredConnectionString == writer.ConnectionString ||
                this == (writerTarget ??= new PostgreSqlDatabaseTarget(writer.ConnectionString, database)));
    }
}
