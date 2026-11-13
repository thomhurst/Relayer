using Microsoft.EntityFrameworkCore.Migrations;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Outbox.EntityFrameworkCore;

/// <summary>Installs the shared PostgreSQL schema from an application-owned EF Core migration.</summary>
public static class PostgreSqlMigrationExtensions
{
    /// <summary>Adds the idempotent PostgreSQL creation script to a migration.</summary>
    /// <remarks>
    /// Call only from a PostgreSQL migration. Relayer owns these tables outside the EF model snapshot.
    /// There is no automatic destructive Down operation. This does not register transaction interceptors.
    /// </remarks>
    public static void CreateRelayerPostgreSqlSchema(this MigrationBuilder migrationBuilder,
        PostgreSqlOutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        if (migrationBuilder.ActiveProvider != "Npgsql.EntityFrameworkCore.PostgreSQL")
            throw new InvalidOperationException("CreateRelayerPostgreSqlSchema requires the Npgsql EF Core provider. Other database schemas are not implemented.");
        migrationBuilder.Sql(PostgreSqlOutboxSchema.CreateScript(options));
    }
}
